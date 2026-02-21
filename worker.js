/**
 * Cloudflare Workers (Pages) - Telegram Bot Entry Point (V5.5 独立配置版)
 * 核心升级：修复全局设置串线问题，为每个群组引入完全独立的设置面板，Add JSON 直导。
 * V5.5.1 性能优化：批量设置查询、并发成员校验、id-pivot随机、ctx.waitUntil写入异步化
 */

/* =========================================================================
 * 模块级常量与缓存（Cloudflare Worker 实例级别，跨请求共享）
 * ========================================================================= */
const SETTING_DEFAULTS = Object.freeze({
  display_mode: 'B',
  anti_repeat: 'true',
  auto_jump: 'true',
  dup_notify: 'false',
  show_success: 'true',
  next_mode: 'replace'
});

// 成员资格 TTL 缓存（60秒），避免重复调用 Telegram getChatMember API
const GROUP_MEMBER_CACHE_TTL_MS = 60_000;
const GROUP_MEMBER_CACHE_MAX = 4096;
const groupMembershipCache = new Map();

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      if (request.method === 'GET' && url.pathname === '/') {
        return await handleSetup(url.origin, env);
      }

      if (request.method === 'POST' && url.pathname === '/webhook') {
        const update = await request.json();
        ctx.waitUntil(handleUpdate(update, env, ctx));
        return new Response('OK', { status: 200 });
      }

      if (request.method === 'POST' && url.pathname === '/api/import') {
        const secret = request.headers.get('Authorization');
        if (env.ADMIN_SECRET && secret !== env.ADMIN_SECRET) return new Response('Unauthorized', { status: 401 });
        const payload = await request.json();
        ctx.waitUntil(handleExternalImport(payload.data, env));
        return new Response(JSON.stringify({ status: 'success', count: payload.data.length }), { status: 200 });
      }

      return new Response('Not Found', { status: 404 });
    } catch (err) {
      console.error('Worker Error:', err);
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};

/* =========================================================================
 * 部署与初始化逻辑
 * ========================================================================= */
async function handleSetup(origin, env) {
  try {
    const initSQL = [
      `CREATE TABLE IF NOT EXISTS config_topics (id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, chat_title TEXT, topic_id INTEGER, category_name TEXT, bound_by INTEGER, created_at DATETIME DEFAULT CURRENT_TIMESTAMP);`,
      `CREATE TABLE IF NOT EXISTS media_library (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER, chat_id INTEGER, topic_id INTEGER, category_name TEXT, view_count INTEGER DEFAULT 0, file_unique_id TEXT, file_id TEXT, media_type TEXT, caption TEXT, added_at DATETIME DEFAULT CURRENT_TIMESTAMP);`,
      `CREATE TABLE IF NOT EXISTS user_favorites (user_id INTEGER, media_id INTEGER, saved_at DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(user_id, media_id));`,
      `CREATE TABLE IF NOT EXISTS last_served (user_id INTEGER PRIMARY KEY, last_media_id INTEGER, served_at INTEGER);`,
      `CREATE TABLE IF NOT EXISTS served_history (media_id INTEGER PRIMARY KEY);`,
      
      // V5.5 核心升级：新建带有 chat_id 的群组独立配置表
      `CREATE TABLE IF NOT EXISTS chat_settings (chat_id INTEGER, key TEXT, value TEXT, PRIMARY KEY(chat_id, key));`,
      // 兼容旧版留存
      `CREATE TABLE IF NOT EXISTS bot_settings (key TEXT PRIMARY KEY, value TEXT);`,
      // V5.5.1 性能索引
      `CREATE INDEX IF NOT EXISTS idx_media_chat_cat_id ON media_library (chat_id, category_name, id);`,
      `CREATE INDEX IF NOT EXISTS idx_media_chat_viewcount ON media_library (chat_id, view_count DESC);`,
      `CREATE INDEX IF NOT EXISTS idx_topics_chat_cat ON config_topics (chat_id, category_name);`,
      `CREATE INDEX IF NOT EXISTS idx_served_history_media ON served_history (media_id);`
    ];

    for (const sql of initSQL) await env.D1.prepare(sql).run();

    const columns = ['file_unique_id', 'file_id', 'media_type', 'caption'];
    for (const col of columns) {
      try { await env.D1.prepare(`ALTER TABLE media_library ADD COLUMN ${col} TEXT;`).run(); } catch (e) {}
    }

    const webhookUrl = `${origin}/webhook`;
    const tgRes = await tgAPI('setWebhook', { url: webhookUrl }, env);
    if (!tgRes.ok) throw new Error('Webhook 注册失败');

    const html = `
      <!DOCTYPE html>
      <html lang="zh-CN">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Bot部署成功喵！</title>
        <style>
          body { font-family: system-ui, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; background-color: #f3f4f6; margin: 0; }
          .card { background: white; padding: 2.5rem 3rem; border-radius: 16px; box-shadow: 0 10px 15px -3px rgba(0,0,0,0.1); text-align: center; max-width: 500px;}
          h1 { color: #10b981; margin-bottom: 0.5rem; }
          p { color: #4b5563; line-height: 1.6; }
          .code-box { background: #f8fafc; padding: 0.5rem; border-radius: 6px; border: 1px solid #e2e8f0; font-family: monospace; word-break: break-all; color: #2563eb; margin: 1rem 0;}
        </style>
      </head>
      <body>
        <div class="card">
          <h1>🎉 籽青 V5.5 部署大成功喵！</h1>
          <p>这里一般放更新介绍，但俺懒得写了喵！<br>Webhook 已经帮主人绑定好啦：</p>
          <div class="code-box">${webhookUrl}</div>
          <p><b>快去群里玩耍吧！QwQ</b></p>
        </div>
      </body>
      </html>
    `;
    return new Response(html, { headers: { 'Content-Type': 'text/html;charset=UTF-8' } });
  } catch (error) {
    return new Response(`部署失败喵: ${error.message}`, { status: 500 });
  }
}

/* =========================================================================
 * 路由与消息处理
 * ========================================================================= */
async function handleUpdate(update, env, ctx) {
  if (update.message) {
    await handleMessage(update.message, env, ctx);
  } else if (update.callback_query) {
    await handleCallback(update.callback_query, env, ctx);
  }
}

async function handleMessage(message, env, ctx) {
  const text = message.text || message.caption || '';
  const chatId = message.chat.id;
  const topicId = message.message_thread_id || null;
  const userId = message.from.id;

  if (text.startsWith('/start')) return sendMainMenu(chatId, topicId, env, userId);

  if (text.startsWith('/help')) {
    const helpText = `📖 **籽青的说明书喵~ (≧∇≦)**\n/start - 唤出籽青的主菜单\n\n**【管理员专属指令喵】**\n/bind &lt;分类名&gt; - 将当前话题绑定为采集库\n/bind_output - 将当前话题设为专属推送展示窗口\n/import_json - 获取关于导入历史消息的说明`;
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: helpText, parse_mode: 'HTML' }, env);
    return;
  }

  if (text.startsWith('/import_json')) {
    const importHelp = `📥 **关于导入历史数据喵**\n\n籽青有两种方法可以吃掉历史数据哦：\n\n1. **直接投喂 (适合 5MB 以内的小包裹)**：直接把 \`.json\` 文件发给籽青，并在文件的说明(Caption)里写上 \`/import 分类名\` 即可！\n2. **脚本投喂 (适合大包裹)**：在电脑上运行配套的 Python 导入脚本，慢慢喂给籽青！QwQ`;
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: importHelp, parse_mode: 'Markdown' }, env);
    return;
  }

  if (text.startsWith('/bind ')) {
    if (!(await isAdmin(chatId, userId, env))) return;
    const category = text.replace('/bind ', '').trim();
    if (!category) return;
    await env.D1.prepare(`INSERT INTO config_topics (chat_id, chat_title, topic_id, category_name, bound_by) VALUES (?, ?, ?, ?, ?)`)
      .bind(chatId, message.chat.title || 'Private', topicId, category, userId).run();
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `绑定成功喵！籽青已将当前话题与分类【${category}】绑定啦！(๑•̀ㅂ•́)و✧` }, env);
    return;
  }

  if (text.startsWith('/bind_output')) {
    if (!(await isAdmin(chatId, userId, env))) return;
    await env.D1.prepare(`INSERT INTO config_topics (chat_id, chat_title, topic_id, category_name, bound_by) VALUES (?, ?, ?, ?, ?)`)
      .bind(chatId, message.chat.title || 'Private', topicId, 'output', userId).run();
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `设置成功喵！籽青以后就在这里发图啦~ QwQ` }, env);
    return;
  }

  // ==== 内置 JSON 直接解析功能 ====
  if (message.document && message.document.file_name && message.document.file_name.endsWith('.json') && text.startsWith('/import ')) {
    if (!(await isAdmin(chatId, userId, env))) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `🚨 呜呜，只有管理员主人才可以给籽青投喂文件哦！` }, env);
    }
    
    const category = text.replace('/import ', '').trim();
    if (!category) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `喵？请在文件说明里写上正确格式，比如：\`/import 分类名\` 哦！` }, env);

    if (message.document.file_size > 5242880) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `🚨 呜呜... 这个包裹太大了（超过 5MB），籽青的肚子装不下会撑爆的！请使用 Python 脚本进行外部导入喵 QwQ` }, env);
    }

    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `📥 收到包裹！籽青正在努力吃掉这个文件，请稍等喵...` }, env);

    try {
      const fileRes = await tgAPI('getFile', { file_id: message.document.file_id }, env);
      const fileData = await fileRes.json();
      if (!fileData.ok) throw new Error("无法从 TG 服务器拉取文件");
      const downloadUrl = `https://api.telegram.org/file/bot${env.BOT_TOKEN_ENV}/${fileData.result.file_path}`;

      const jsonRes = await fetch(downloadUrl);
      const jsonData = await jsonRes.json();
      const messages = jsonData.messages || [];
      
      let validMedia = [];
      for (const msg of messages) {
        if (msg.type !== 'message') continue;
        let mediaType = null;
        if (msg.photo) mediaType = 'photo';
        else if (msg.media_type === 'video_file') mediaType = 'video';
        else if (msg.media_type === 'animation') mediaType = 'animation';
        else if (msg.media_type) mediaType = 'document';

        if (!mediaType) continue;

        let caption = "";
        if (Array.isArray(msg.text)) {
          caption = msg.text.map(t => typeof t === 'string' ? t : (t.text || '')).join('');
        } else if (typeof msg.text === 'string') {
          caption = msg.text;
        }

        validMedia.push({
          message_id: msg.id,
          chat_id: chatId,
          topic_id: null,
          category_name: category,
          file_unique_id: `import_${chatId}_${msg.id}`, 
          file_id: '',
          media_type: mediaType,
          caption: caption.substring(0, 100) 
        });
      }

      if (validMedia.length === 0) {
        return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `❓ 哎呀，籽青在这个文件里没有找到任何图片或视频记录喵。` }, env);
      }

      let successCount = 0;
      for (let i = 0; i < validMedia.length; i += 50) {
        const batch = validMedia.slice(i, i + 50);
        const stmts = batch.map(item => {
          return env.D1.prepare(`INSERT INTO media_library (message_id, chat_id, topic_id, category_name, file_unique_id, file_id, media_type, caption) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
            .bind(item.message_id, item.chat_id, item.topic_id, item.category_name, item.file_unique_id, item.file_id, item.media_type, item.caption);
        });
        await env.D1.batch(stmts);
        successCount += batch.length;
      }

      await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `🎉 嗝~ 吃饱啦！成功从文件里导入了 ${successCount} 条【${category}】的记录喵！` }, env);
    } catch (err) {
      await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `❌ 呜呜，籽青吃坏肚子了，导入失败喵：${err.message}` }, env);
    }
    return; 
  }

  // ==== 日常媒体收录拦截 ====
  let mediaInfo = extractMediaInfo(message);
  if (mediaInfo.fileUniqueId) {
    const query = await env.D1.prepare(`SELECT category_name FROM config_topics WHERE chat_id = ? AND (topic_id = ? OR topic_id IS NULL) AND category_name != 'output' LIMIT 1`).bind(chatId, topicId).first();
    if (query && query.category_name) {
      const existing = await env.D1.prepare(`SELECT id FROM media_library WHERE file_unique_id = ? AND chat_id = ? LIMIT 1`).bind(mediaInfo.fileUniqueId, chatId).first();
      if (existing) {
        const notify = await getSetting(chatId, 'dup_notify', env);
        if (notify === 'true') await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id, text: "哎呀，籽青发现这个内容之前已经收录过啦喵~" }, env);
        return; 
      }
      await env.D1.prepare(`INSERT INTO media_library (message_id, chat_id, topic_id, category_name, file_unique_id, file_id, media_type, caption) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
        .bind(message.message_id, chatId, topicId, query.category_name, mediaInfo.fileUniqueId, mediaInfo.fileId, mediaInfo.type, message.caption || '').run();
    }
  }
}

function extractMediaInfo(message) {
  let info = { fileUniqueId: null, fileId: null, type: null };
  if (message.photo && message.photo.length > 0) {
    const p = message.photo[message.photo.length - 1];
    info = { fileUniqueId: p.file_unique_id, fileId: p.file_id, type: 'photo' };
  } else if (message.video) {
    info = { fileUniqueId: message.video.file_unique_id, fileId: message.video.file_id, type: 'video' };
  } else if (message.document) {
    info = { fileUniqueId: message.document.file_unique_id, fileId: message.document.file_id, type: 'document' };
  } else if (message.animation) {
    info = { fileUniqueId: message.animation.file_unique_id, fileId: message.animation.file_id, type: 'animation' };
  }
  return info;
}

/* =========================================================================
 * 回调交互处理
 * ========================================================================= */
async function handleCallback(callback, env, ctx) {
  const data = callback.data;
  const userId = callback.from.id;
  const chatId = callback.message.chat.id;
  const msgId = callback.message.message_id;
  const topicId = callback.message.message_thread_id || null;
  const cbId = callback.id;

  if (data === 'main_menu') {
    await Promise.all([
      editMainMenu(chatId, msgId, env, userId),
      tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env)
    ]);
  } else if (data === 'main_menu_new') {
    await Promise.all([
      sendMainMenu(chatId, topicId, env, userId),
      tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env)
    ]);
  } else if (data === 'start_random') {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    await showCategories(chatId, msgId, env, userId);
  } else if (data.startsWith('random_') || data.startsWith('next_')) {
    const action = data.startsWith('random_') ? 'random_' : 'next_';
    const params = data.replace(action, '').split('|');
    const category = params[0];
    const sourceChatId = params.length > 1 ? parseInt(params[1]) : chatId;

    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "籽青正在为你抽取喵..." }, env);
    await sendRandomMedia(userId, chatId, msgId, topicId, category, sourceChatId, action === 'next_', env, ctx);
  }

  else if (data.startsWith('fav_add_')) {
    await handleAddFavorite(userId, cbId, parseInt(data.replace('fav_add_', '')), env);
  } else if (data === 'favorites' || data.startsWith('fav_page_')) {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    const page = data === 'favorites' ? 0 : parseInt(data.replace('fav_page_', ''));
    await showFavoritesList(chatId, msgId, userId, page, env);
  } else if (data.startsWith('fav_view_')) {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    await viewFavorite(chatId, topicId, parseInt(data.replace('fav_view_', '')), env);
  } else if (data.startsWith('fav_del_')) {
    await env.D1.prepare(`DELETE FROM user_favorites WHERE user_id = ? AND media_id = ?`).bind(userId, parseInt(data.replace('fav_del_', ''))).run();
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "已从收藏夹移除喵！" }, env);
    await showFavoritesList(chatId, msgId, userId, 0, env);
  }

  else if (data === 'leaderboard' || data.startsWith('leader_page_')) {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    const page = data === 'leaderboard' ? 0 : parseInt(data.replace('leader_page_', ''));
    await showLeaderboard(chatId, msgId, page, env);
  }

  else if (data.startsWith('set_')) {
    if (chatId > 0) return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "喵！只能在群组内使用设置面板哦！", show_alert: true }, env);
    if (!(await isAdmin(chatId, userId, env))) {
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "呜呜，只有管理员才能调整籽青哦！", show_alert: true }, env);
      return;
    }

    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);

    if (data === 'set_main') await showSettingsMain(chatId, msgId, env);
    else if (data === 'set_toggle_mode') await toggleSetting('display_mode', env, chatId, msgId, ['A', 'B']);
    else if (data === 'set_toggle_repeat') await toggleSetting('anti_repeat', env, chatId, msgId, ['true', 'false']);
    else if (data === 'set_toggle_jump') await toggleSetting('auto_jump', env, chatId, msgId, ['true', 'false']);
    else if (data === 'set_toggle_dup') await toggleSetting('dup_notify', env, chatId, msgId, ['true', 'false']);
    else if (data === 'set_toggle_success') await toggleSetting('show_success', env, chatId, msgId, ['true', 'false']);
    else if (data === 'set_toggle_nextmode') await toggleSetting('next_mode', env, chatId, msgId, ['replace', 'new']);
    else if (data === 'set_stats') await showStats(chatId, msgId, env);
    else if (data === 'set_unbind_list') await showUnbindList(chatId, msgId, env);
    else if (data.startsWith('set_unbind_do_')) {
      await env.D1.prepare(`DELETE FROM config_topics WHERE id = ? AND chat_id = ?`).bind(parseInt(data.replace('set_unbind_do_', '')), chatId).run();
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "解绑成功喵！", show_alert: true }, env);
      await showUnbindList(chatId, msgId, env);
    }

    else if (data === 'set_danger_zone') {
      const text = "⚠️ **危险操作区**\n\n这里的操作仅对当前群组生效，且不可逆喵！";
      const keyboard = [[{ text: "🧨 清空本群数据统计", callback_data: "set_clear_stats_1" }], [{ text: "🚨 彻底清空本群媒体库", callback_data: "set_clear_media_1" }], [{ text: "⬅️ 返回安全区", callback_data: "set_main" }]];
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
    }
    else if (data === 'set_clear_stats_1') {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "⚠️ 确定仅清空本群统计数据吗喵？", reply_markup: { inline_keyboard: [[{ text: "🔴 确认清空 (第1次)", callback_data: "set_clear_stats_2" }], [{ text: "⬅️ 返回", callback_data: "set_main" }]] } }, env);
    } else if (data === 'set_clear_stats_2') {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🧨 **最后警告**：即将清空本群浏览量喵！", parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: "☠️ 彻底清空！", callback_data: "set_clear_stats_do" }], [{ text: "⬅️ 算了", callback_data: "set_main" }]] } }, env);
    } else if (data === 'set_clear_stats_do') {
      await env.D1.prepare(`UPDATE media_library SET view_count = 0 WHERE chat_id = ?`).bind(chatId).run();
      await env.D1.prepare(`DELETE FROM served_history WHERE media_id IN (SELECT id FROM media_library WHERE chat_id = ?)`).bind(chatId).run();
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "当前群组统计重置完毕喵！", show_alert: true }, env);
      await showSettingsMain(chatId, msgId, env);
    }
    else if (data === 'set_clear_media_1') {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🚨 **高危警告**\n\n即将清空【本群收录的所有媒体】喵！", parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: "🩸 我确定要删除本群全部媒体", callback_data: "set_clear_media_2" }], [{ text: "⬅️ 返回安全区", callback_data: "set_main" }]] } }, env);
    } else if (data === 'set_clear_media_2') {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🌋 **最终警告**\n\n一旦按下无法恢复喵！真的要清空吗？", parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: "💥 毁天灭地！", callback_data: "set_clear_media_do" }], [{ text: "⬅️ 放弃操作", callback_data: "set_main" }]] } }, env);
    } else if (data === 'set_clear_media_do') {
      await env.D1.prepare(`DELETE FROM user_favorites WHERE media_id IN (SELECT id FROM media_library WHERE chat_id = ?)`).bind(chatId).run();
      await env.D1.prepare(`DELETE FROM served_history WHERE media_id IN (SELECT id FROM media_library WHERE chat_id = ?)`).bind(chatId).run();
      await env.D1.prepare(`DELETE FROM media_library WHERE chat_id = ?`).bind(chatId).run();
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "当前群组媒体库已被彻底清空喵！", show_alert: true }, env);
      await showSettingsMain(chatId, msgId, env);
    }
  }
}

/* =========================================================================
 * UI 流转逻辑 (包含身份鉴权)
 * ========================================================================= */
async function sendMainMenu(chatId, topicId, env, userId) {
  if (chatId > 0) {
    const allowedGroups = await getUserAllowedGroups(userId, env);
    if (allowedGroups.length === 0) {
      await tgAPI('sendMessage', { chat_id: chatId, text: "⛔ 喵呜... 籽青查了一下，你目前还没有加入任何授权群组呢，不能给你看图库哦 QwQ", parse_mode: 'HTML' }, env);
      return;
    }
  }
  await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "你好呀！我是籽青喵 (≧∇≦) 请问今天想看点什么呢？", reply_markup: getMainMenuMarkup() }, env);
}

async function editMainMenu(chatId, msgId, env, userId) {
  if (chatId > 0) {
    const allowedGroups = await getUserAllowedGroups(userId, env);
    if (allowedGroups.length === 0) {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "⛔ 喵... 你好像退群了呢，籽青已经把菜单收回去了哦！" }, env);
      return;
    }
  }
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "这是籽青的主菜单，请选择喵：", reply_markup: getMainMenuMarkup() }, env);
}

function getMainMenuMarkup() {
  return { inline_keyboard: [[{ text: "🎲 开始随机", callback_data: "start_random" }], [{ text: "🏆 本群排行", callback_data: "leaderboard" }, { text: "📁 收藏夹", callback_data: "favorites" }], [{ text: "⚙️ 籽青设置 (限管理)", callback_data: "set_main" }]] };
}

async function showCategories(chatId, msgId, env, userId) {
  let keyboard = [];
  
  if (chatId < 0) {
    const localRes = await env.D1.prepare(`SELECT DISTINCT category_name FROM config_topics WHERE category_name != 'output' AND chat_id = ?`).bind(chatId).all();
    if (localRes.results) {
      localRes.results.forEach(row => keyboard.push([{ text: `📂 ${row.category_name}`, callback_data: `random_${row.category_name}|${chatId}` }]));
    }
  } else {
    const allowedGroups = await getUserAllowedGroups(userId, env);
    if (allowedGroups.length > 0) {
      const placeholders = allowedGroups.map(() => '?').join(', ');
      const { results } = await env.D1.prepare(
        `SELECT DISTINCT chat_id, chat_title, category_name FROM config_topics WHERE category_name != 'output' AND chat_id IN (${placeholders}) ORDER BY chat_title, category_name`
      ).bind(...allowedGroups).all();
      for (const row of (results || [])) {
        keyboard.push([{ text: `📂 [${row.chat_title}] ${row.category_name}`, callback_data: `random_${row.category_name}|${row.chat_id}` }]);
      }
    }
  }

  if (keyboard.length === 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "呜呜，当前群组还没有绑定任何分类喵，管理员请使用 /bind 绑定哦！", reply_markup: getBackMarkup() }, env);

  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "main_menu" }]);
  const text = chatId < 0 ? "请选择您感兴趣的分类喵：" : "👇 以下是您所在群组的专属图库喵：";
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: text, reply_markup: { inline_keyboard: keyboard } }, env);
}

// ==== 核心抽取与展现逻辑 (融合 方案A: 失效自动清理 & 群组炸群连坐清理) ====
async function sendRandomMedia(userId, chatId, msgId, topicId, category, sourceChatId, isNext, env, ctx) {
  if (chatId > 0) {
    const inGroup = await isUserInGroup(sourceChatId, userId, env);
    if (!inGroup) {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🚨 喵！大骗子！籽青发现你已经退群啦，休想再拿之前的菜单偷看！(｀・ω・´)" }, env);
      return;
    }
  }

  let outChatId = chatId;
  let outTopicId = topicId;

  if (chatId < 0) {
    const output = await env.D1.prepare(`SELECT chat_id, topic_id FROM config_topics WHERE category_name = 'output' AND chat_id = ? LIMIT 1`).bind(chatId).first();
    if (!output) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `喵？管理员还没设置本群输出话题呢，请用 /bind_output 设置！` }, env);
    outChatId = output.chat_id;
    outTopicId = output.topic_id;
  }

  // P1: 批量读取所有设置，1次 D1 查询替代 5次
  const settings = await getSettingsBatch(sourceChatId, ['display_mode', 'anti_repeat', 'auto_jump', 'show_success', 'next_mode'], env);
  const mode = settings.display_mode;
  const useAntiRepeat = settings.anti_repeat === 'true';
  const autoJump = settings.auto_jump === 'true';
  const showSuccess = settings.show_success === 'true';
  const nextMode = settings.next_mode || 'replace';
  const now = Date.now();

  // 连点防刷退回逻辑
  if (isNext) {
    const last = await env.D1.prepare(`SELECT * FROM last_served WHERE user_id = ?`).bind(userId).first();
    if (last && (now - last.served_at) < 30000) {
      // P3: 非关键写入异步化
      ctx.waitUntil(Promise.all([
        env.D1.prepare(`UPDATE media_library SET view_count = MAX(0, view_count - 1) WHERE id = ?`).bind(last.last_media_id).run(),
        useAntiRepeat ? env.D1.prepare(`DELETE FROM served_history WHERE media_id = ?`).bind(last.last_media_id).run() : Promise.resolve()
      ]));
    }
  }

  // 🌟 方案 A 自动重试与体检循环 (最多重试 3 次，防止 CF Worker 超时)
  let attempts = 0;
  let foundValid = false;
  let media = null;
  let newSentMessageId = null;

  while (attempts < 3 && !foundValid) {
    attempts++;

    // 1. P1: id-pivot 随机策略替代 ORDER BY RANDOM() 全表扫描
    media = await selectRandomMedia(category, sourceChatId, useAntiRepeat, env);

    // 如果防重库空了，重置防重库再捞一次
    if (!media && useAntiRepeat) {
      const totalCheck = await env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE category_name = ? AND chat_id = ?`).bind(category, sourceChatId).first();
      if (totalCheck && totalCheck.c > 0) {
        await env.D1.prepare(`DELETE FROM served_history WHERE media_id IN (SELECT id FROM media_library WHERE category_name = ? AND chat_id = ?)`).bind(category, sourceChatId).run();
        await tgAPI('sendMessage', { chat_id: outChatId, message_thread_id: outTopicId, text: `🎉 哇哦，【${category}】的内容全看光了！籽青已重置防重库喵~` }, env);
        media = await selectRandomMedia(category, sourceChatId, false, env);
      }
    }

    if (!media) {
      await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `呜呜，该分类里还没有内容呢喵~` }, env);
      return;
    }

    // 2. 原地替换：尝试删除上一次的旧消息卡片
    if (isNext && nextMode === 'replace' && attempts === 1) {
      try { await tgAPI('deleteMessage', { chat_id: outChatId, message_id: msgId }, env); } catch (e) {}
    }

    // 3. 尝试发送给用户 (探活核心)
    const actionKeyboard = [[{ text: "⏭️ 换一个喵", callback_data: `next_${category}|${sourceChatId}` }, { text: "❤️ 收藏", callback_data: `fav_add_${media.id}` }]];
    const originalDeepLink = makeDeepLink(media.chat_id, media.message_id);

    let res, data;
    if (mode === 'A') {
      res = await tgAPI('forwardMessage', { chat_id: outChatId, message_thread_id: outTopicId, from_chat_id: media.chat_id, message_id: media.message_id }, env);
      data = await res.json();
      if(data.ok) {
        newSentMessageId = data.result.message_id;
        actionKeyboard.push([{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]);
        await tgAPI('sendMessage', { chat_id: outChatId, message_thread_id: outTopicId, reply_to_message_id: newSentMessageId, text: "👆 可以点这里操作喵：", reply_markup: { inline_keyboard: actionKeyboard } }, env);
      }
    } else {
      actionKeyboard.unshift([{ text: "🔗 去原记录围观", url: originalDeepLink }]);
      actionKeyboard.push([{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]);
      res = await tgAPI('copyMessage', { chat_id: outChatId, message_thread_id: outTopicId, from_chat_id: media.chat_id, message_id: media.message_id, reply_markup: { inline_keyboard: actionKeyboard } }, env);
      data = await res.json();
      if(data.ok) newSentMessageId = data.result.message_id;
    }

    // 4. 分析探活结果
    if (data.ok) {
      foundValid = true;
    } else {
      const errDesc = data.description || '';
      console.error("探活报错喵:", errDesc);

      if (errDesc.includes('chat not found') || errDesc.includes('bot was kicked') || errDesc.includes('channel not found')) {
        await env.D1.prepare(`DELETE FROM media_library WHERE chat_id = ?`).bind(media.chat_id).run();
        await env.D1.prepare(`DELETE FROM config_topics WHERE chat_id = ?`).bind(media.chat_id).run();
      } else {
        await env.D1.prepare(`DELETE FROM media_library WHERE id = ?`).bind(media.id).run();
      }
    }
  }

  // ==== 循环结束后的收尾工作 ====
  if (!foundValid) {
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🧹 呼... 连续抽到好多失效图片，籽青已经把坏数据打扫干净啦，请主人再点一次重抽喵！" }, env);
  }

  // P3: 统计写入全部异步化，不阻塞响应
  ctx.waitUntil(Promise.all([
    useAntiRepeat ? env.D1.prepare(`INSERT OR IGNORE INTO served_history (media_id) VALUES (?)`).bind(media.id).run() : Promise.resolve(),
    env.D1.prepare(`INSERT INTO last_served (user_id, last_media_id, served_at) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET last_media_id=excluded.last_media_id, served_at=excluded.served_at`).bind(userId, media.id, now).run(),
    env.D1.prepare(`UPDATE media_library SET view_count = view_count + 1 WHERE id = ?`).bind(media.id).run()
  ]));

  // 成功抽取的反馈提示
  if (!isNext && chatId < 0) {
    if (showSuccess) {
      const jumpToOutputLink = newSentMessageId ? makeDeepLink(outChatId, newSentMessageId) : null;
      const jumpKeyboard = jumpToOutputLink && autoJump
        ? [[{ text: "🚀 飞去看看", url: jumpToOutputLink }], [{ text: "🏠 返回", callback_data: "main_menu" }]]
        : [[{ text: "🏠 返回", callback_data: "main_menu" }]];
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: `🎉 抽取成功啦喵！已发送至输出话题。`, reply_markup: { inline_keyboard: jumpKeyboard } }, env);
    } else {
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "抽取成功喵！" }, env);
    }
  }
}

async function showLeaderboard(chatId, msgId, page, env) {
  const limit = 5;
  const offset = page * limit;
  if (chatId > 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "喵，私聊模式暂不支持查看群排行哦，请在群组内使用 QwQ", reply_markup: getBackMarkup() }, env);

  const [leaderData, totalRes] = await Promise.all([
    env.D1.prepare(`SELECT chat_id, message_id, category_name, view_count, caption FROM media_library WHERE view_count > 0 AND chat_id = ? ORDER BY view_count DESC LIMIT ? OFFSET ?`).bind(chatId, limit, offset).all(),
    env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE view_count > 0 AND chat_id = ?`).bind(chatId).first()
  ]);
  const results = leaderData.results;
  
  let text = "🏆 **本群浏览量排行榜喵**\n\n";
  if (!results || results.length === 0) {
    text += "当前群组还没有产生播放数据呢~";
  } else {
    results.forEach((row, idx) => { 
      const preview = row.caption ? row.caption.substring(0, 15) + '...' : '媒体记录';
      text += `${offset + idx + 1}. [${row.category_name}] <a href="${makeDeepLink(row.chat_id, row.message_id)}">${preview}</a> - 浏览: ${row.view_count}\n`; 
    });
  }

  const keyboard = [];
  const navRow = [];
  if (page > 0) navRow.push({ text: "⬅️ 上一页", callback_data: `leader_page_${page - 1}` });
  if (offset + limit < totalRes.c) navRow.push({ text: "下一页 ➡️", callback_data: `leader_page_${page + 1}` });
  if (navRow.length > 0) keyboard.push(navRow);
  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "main_menu" }]);

  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'HTML', disable_web_page_preview: true, reply_markup: { inline_keyboard: keyboard } }, env);
}

async function handleAddFavorite(userId, cbId, mediaId, env) {
  try { 
    await env.D1.prepare(`INSERT INTO user_favorites (user_id, media_id) VALUES (?, ?)`).bind(userId, mediaId).run(); 
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "收藏成功喵！籽青帮你记下来啦~ ❤️", show_alert: true }, env); 
  } catch (e) { 
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "喵？你已经收藏过这个啦~", show_alert: true }, env); 
  }
}

async function showFavoritesList(chatId, msgId, userId, page, env) {
  const limit = 5;
  const offset = page * limit;
  const { results } = await env.D1.prepare(`SELECT f.media_id, m.media_type, m.caption FROM user_favorites f LEFT JOIN media_library m ON f.media_id = m.id WHERE f.user_id = ? ORDER BY f.saved_at DESC LIMIT ? OFFSET ?`).bind(userId, limit, offset).all();
  const totalRes = await env.D1.prepare(`SELECT count(*) as c FROM user_favorites WHERE user_id = ?`).bind(userId).first();
  
  if (!results || results.length === 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "你的收藏夹空空如也哦喵~", reply_markup: getBackMarkup() }, env);
  
  const keyboard = results.map((r, i) => {
    const typeIcon = r.media_type === 'video' ? '🎬' : (r.media_type === 'photo' ? '🖼️' : '📁');
    const title = r.caption ? r.caption.substring(0, 15) : '记录';
    return [
      { text: `${typeIcon} ${title}`, callback_data: `fav_view_${r.media_id}` }, 
      { text: `❌ 移除`, callback_data: `fav_del_${r.media_id}` }
    ];
  });

  const navRow = [];
  if (page > 0) navRow.push({ text: "⬅️ 上一页", callback_data: `fav_page_${page - 1}` });
  if (offset + limit < totalRes.c) navRow.push({ text: "下一页 ➡️", callback_data: `fav_page_${page + 1}` });
  if (navRow.length > 0) keyboard.push(navRow);
  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "main_menu" }]);
  
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: `📁 **主人的私有收藏夹** (共 ${totalRes.c} 条)`, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
}

async function viewFavorite(chatId, topicId, mediaId, env) {
  const media = await env.D1.prepare(`SELECT * FROM media_library WHERE id = ?`).bind(mediaId).first();
  if (media) await tgAPI('copyMessage', { chat_id: chatId, message_thread_id: topicId, from_chat_id: media.chat_id, message_id: media.message_id }, env);
}

// ==== V5.5 专属设置看板 (基于 chat_id 获取独立配置) ====
async function showSettingsMain(chatId, msgId, env) {
  // P1: 批量读取所有设置，1次 D1 查询替代 6次
  const settings = await getSettingsBatch(chatId, ['display_mode', 'anti_repeat', 'auto_jump', 'dup_notify', 'show_success', 'next_mode'], env);
  const mode = settings.display_mode;
  const repeat = settings.anti_repeat;
  const jump = settings.auto_jump;
  const dup = settings.dup_notify;
  const showSuccess = settings.show_success;
  const nextMode = settings.next_mode;
  
  const text = "⚙️ **本群的独立控制面板喵**\n\n请主人调整下方的功能开关：";
  const keyboard = [
    [{ text: `🔀 展现形式: ${mode === 'A' ? 'A(原生转发)' : 'B(复制+链接)'}`, callback_data: "set_toggle_mode" }],
    [{ text: `🔁 防重库机制: ${repeat === 'true' ? '✅ 已开启' : '❌ 未开启'}`, callback_data: "set_toggle_repeat" }],
    [{ text: `🔕 重复收录提示: ${dup === 'true' ? '📢 消息提醒' : '🔇 静默拦截'}`, callback_data: "set_toggle_dup" }],
    [{ text: `🔄 '换一个'模式: ${nextMode === 'replace' ? '🖼️ 原地替换(删旧发新)' : '💬 发新消息(保留历史)'}`, callback_data: "set_toggle_nextmode" }],
    [{ text: `🔔 抽取成功提示: ${showSuccess === 'true' ? '✅ 开启' : '❌ 关闭'}`, callback_data: "set_toggle_success" }],
    [{ text: `🚀 抽取后生成跳转: ${jump === 'true' ? '✅ 开启' : '❌ 关闭'}`, callback_data: "set_toggle_jump" }],
    [{ text: "🗑️ 管理本群解绑", callback_data: "set_unbind_list" }, { text: "📊 本群数据看板", callback_data: "set_stats" }],
    [{ text: "⚠️ 危险操作区 (清空本群数据)", callback_data: "set_danger_zone" }],
    [{ text: "🏠 返回主菜单", callback_data: "main_menu" }]
  ];
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
}

// ==== V5.5 更新：保存独立配置 ====
async function toggleSetting(key, env, chatId, msgId, values) {
  const current = await getSetting(chatId, key, env);
  const valCurrent = current === null ? values[0] : current;
  const next = valCurrent === values[0] ? values[1] : values[0];
  
  // 插入带有 chat_id 的设置，遇到冲突就更新 value
  await env.D1.prepare(`INSERT INTO chat_settings (chat_id, key, value) VALUES (?, ?, ?) ON CONFLICT(chat_id, key) DO UPDATE SET value=excluded.value`).bind(chatId, key, next).run();
  
  await showSettingsMain(chatId, msgId, env);
}

async function showUnbindList(chatId, msgId, env) {
  const { results } = await env.D1.prepare(`SELECT id, chat_title, category_name FROM config_topics WHERE chat_id = ?`).bind(chatId).all();
  if (!results || results.length === 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "本群目前没有绑定任何记录喵~", reply_markup: { inline_keyboard: [[{text: "返回设置", callback_data: "set_main"}]] } }, env);
  const keyboard = results.map(r => [{ text: `🗑️ 解绑 [${r.category_name}]`, callback_data: `set_unbind_do_${r.id}` }]);
  keyboard.push([{ text: "⬅️ 返回设置", callback_data: "set_main" }]);
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "点击对应按钮解除本群的话题绑定喵：", reply_markup: { inline_keyboard: keyboard } }, env);
}

async function showStats(chatId, msgId, env) {
  const [mediaRes, topicRes] = await Promise.all([
    env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE chat_id = ?`).bind(chatId).first(),
    env.D1.prepare(`SELECT count(*) as c FROM config_topics WHERE chat_id = ?`).bind(chatId).first()
  ]);
  const mediaCount = mediaRes?.c || 0;
  const topicCount = topicRes?.c || 0;
  const text = `📊 **本群数据看板喵**\n\n- 本群收录媒体: **${mediaCount}** 条\n- 本群绑定话题: **${topicCount}** 个`;
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{text: "⬅️ 返回设置", callback_data: "set_main"}]] } }, env);
}

function getBackMarkup() {
  return { inline_keyboard: [[{ text: "🏠 返回主菜单", callback_data: "main_menu" }]] };
}

/* =========================================================================
 * 工具、API 与 身份鉴权拦截
 * ========================================================================= */
async function getUserAllowedGroups(userId, env) {
  const { results } = await env.D1.prepare(`SELECT DISTINCT chat_id FROM config_topics WHERE chat_id < 0`).all();
  if (!results || results.length === 0) return [];

  // P0: 并发检查所有群组，替代串行 for loop
  const checks = results.map(row =>
    isUserInGroup(row.chat_id, userId, env).then(inGroup => inGroup ? row.chat_id : null)
  );
  return (await Promise.all(checks)).filter(id => id !== null);
}

async function isUserInGroup(groupId, userId, env) {
  // P0: TTL 缓存，避免对同一用户/群组重复调用 Telegram API
  const cacheKey = `${groupId}:${userId}`;
  const now = Date.now();
  const cached = groupMembershipCache.get(cacheKey);
  if (cached && cached.expiresAt > now) return cached.value;

  const res = await tgAPI('getChatMember', { chat_id: groupId, user_id: userId }, env);
  const data = await res.json();
  const inGroup = data.ok && ['creator', 'administrator', 'member', 'restricted'].includes(data.result.status);

  // 写入缓存，LRU 超限时淘汰最旧条目
  if (groupMembershipCache.size >= GROUP_MEMBER_CACHE_MAX) {
    groupMembershipCache.delete(groupMembershipCache.keys().next().value);
  }
  groupMembershipCache.set(cacheKey, { value: inGroup, expiresAt: now + GROUP_MEMBER_CACHE_TTL_MS });

  return inGroup;
}

async function handleExternalImport(dataBatch, env) {
  if (!dataBatch || !Array.isArray(dataBatch)) return;
  const stmts = dataBatch.map(item => {
    return env.D1.prepare(`INSERT INTO media_library (message_id, chat_id, topic_id, category_name, file_unique_id, file_id, media_type, caption) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
      .bind(item.message_id, item.chat_id || 0, item.topic_id || null, item.category_name, item.file_unique_id, item.file_id, item.media_type, item.caption || '');
  });
  if (stmts.length > 0) await env.D1.batch(stmts);
}

async function tgAPI(method, payload, env) {
  return fetch(`https://api.telegram.org/bot${env.BOT_TOKEN_ENV}/${method}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
  });
}

// ==== V5.5 更新：支持基于 Chat ID 读取独立默认配置 ====
async function getSetting(chatId, key, env) {
  const res = await env.D1.prepare(`SELECT value FROM chat_settings WHERE chat_id = ? AND key = ?`).bind(chatId, key).first();
  if (res) return res.value;
  return SETTING_DEFAULTS[key] ?? null;
}

// P1: 批量读取多个设置，单次 D1 查询
async function getSettingsBatch(chatId, keys, env) {
  const uniqueKeys = [...new Set(keys)];
  const placeholders = uniqueKeys.map(() => '?').join(', ');
  const { results } = await env.D1.prepare(
    `SELECT key, value FROM chat_settings WHERE chat_id = ? AND key IN (${placeholders})`
  ).bind(chatId, ...uniqueKeys).all();
  const out = {};
  for (const k of uniqueKeys) out[k] = SETTING_DEFAULTS[k] ?? null;
  for (const row of (results || [])) out[row.key] = row.value;
  return out;
}

// P1: id-pivot 随机策略，替代 ORDER BY RANDOM() 全表扫描
// 原理：随机选取一个 id pivot，优先找 id >= pivot 的第一条，找不到则回绕找 id < pivot 的第一条
async function selectRandomMedia(category, sourceChatId, useAntiRepeat, env) {
  const maxRow = await env.D1.prepare(
    `SELECT MAX(id) AS max_id FROM media_library WHERE category_name = ? AND chat_id = ?`
  ).bind(category, sourceChatId).first();
  if (!maxRow || maxRow.max_id === null) return null;

  const pivot = Math.floor(Math.random() * maxRow.max_id) + 1;
  const antiClause = useAntiRepeat
    ? `AND NOT EXISTS (SELECT 1 FROM served_history sh WHERE sh.media_id = m.id)`
    : '';

  // 先找 id >= pivot 的第一条
  let media = await env.D1.prepare(
    `SELECT * FROM media_library m WHERE m.category_name = ? AND m.chat_id = ? ${antiClause} AND m.id >= ? ORDER BY m.id LIMIT 1`
  ).bind(category, sourceChatId, pivot).first();

  if (media) return media;

  // 回绕：找 id < pivot 的最后一条（按 id 升序取第一条等价）
  return env.D1.prepare(
    `SELECT * FROM media_library m WHERE m.category_name = ? AND m.chat_id = ? ${antiClause} AND m.id < ? ORDER BY m.id LIMIT 1`
  ).bind(category, sourceChatId, pivot).first();
}

async function isAdmin(chatId, userId, env) {
  if (chatId > 0) return true;
  const res = await tgAPI('getChatMember', { chat_id: chatId, user_id: userId }, env);
  const data = await res.json();
  return data.ok && (data.result.status === 'administrator' || data.result.status === 'creator');
}

function makeDeepLink(chatId, messageId) {
  return `https://t.me/c/${String(chatId).replace('-100', '')}/${messageId}`;
}
