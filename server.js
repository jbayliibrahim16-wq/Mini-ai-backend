// Mini AI Backend - OpenRouter Integration
// Express.js server with streaming chat support

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 10000;

// Middleware
app.get('/', (req, res) => {
  res.json({ 
    status: 'Live', 
    message: 'Mini AI Backend شغال 🔥',
    time: new Date()
  });
});
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb' }));

// Environment variables
const OPENROUTER_API_KEY = process.env.OPENROUTER_KEY;
const OPENROUTER_BASE_URL = 'https://openrouter.ai/api/v1/chat/completions';
const MODEL = 'google/gemini-flash-1.5';
const ADMIN_SECRET = process.env.ADMIN_SECRET_KEY || 'admin123';

// System prompt
const SYSTEM_PROMPT = `You are Mini AI. You are savage, funny, and helpful. 
Never say "As an AI language model" or similar disclaimers.
Block illegal content only. Be direct and authentic.
Respond in the same language as the user.`;

// Database setup
const dbPath = path.join(__dirname, 'mini_ai.db');
const db = new sqlite3.Database(dbPath, (err) => {
  if (err) console.error('Database error:', err);
  else console.log('✅ Database connected');
});

// Initialize database tables
db.serialize(() => {
  // Users table
  db.run(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  // Chats table
  db.run(`CREATE TABLE IF NOT EXISTS chats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    title TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(user_id) REFERENCES users(id)
  )`);

  // Messages table
  db.run(`CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(chat_id) REFERENCES chats(id),
    FOREIGN KEY(user_id) REFERENCES users(id)
  )`);

  // Files table
  db.run(`CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL,
    filename TEXT,
    file_type TEXT,
    file_data BLOB,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(message_id) REFERENCES messages(id)
  )`);
});

// Helper: Run async DB operations
const dbRun = (sql, params = []) => {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function(err) {
      if (err) reject(err);
      else resolve({ id: this.lastID, changes: this.changes });
    });
  });
};

const dbGet = (sql, params = []) => {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
};

const dbAll = (sql, params = []) => {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows || []);
    });
  });
};

// Routes

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Create or get user
app.post('/api/users', async (req, res) => {
  try {
    const { username } = req.query;
    if (!username) {
      return res.status(400).json({ error: 'Username required' });
    }

    // Check if user exists
    let user = await dbGet('SELECT * FROM users WHERE username = ?', [username]);
    
    // Create if doesn't exist
    if (!user) {
      const result = await dbRun('INSERT INTO users (username) VALUES (?)', [username]);
      user = { id: result.id, username };
    }

    res.json({ id: user.id, username: user.username });
  } catch (error) {
    console.error('User creation error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Create chat
app.post('/api/chats', async (req, res) => {
  try {
    const { user_id, title } = req.body;
    if (!user_id) {
      return res.status(400).json({ error: 'user_id required' });
    }

    const result = await dbRun(
      'INSERT INTO chats (user_id, title) VALUES (?, ?)',
      [user_id, title || 'New Chat']
    );

    res.json({ id: result.id, user_id, title: title || 'New Chat' });
  } catch (error) {
    console.error('Chat creation error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get user chats
app.get('/api/chats/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const chats = await dbAll(
      'SELECT * FROM chats WHERE user_id = ? ORDER BY created_at DESC',
      [userId]
    );
    res.json(chats);
  } catch (error) {
    console.error('Get chats error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get chat messages
app.get('/api/chats/:chatId/messages', async (req, res) => {
  try {
    const { chatId } = req.params;
    const messages = await dbAll(
      'SELECT * FROM messages WHERE chat_id = ? ORDER BY created_at ASC',
      [chatId]
    );
    res.json(messages);
  } catch (error) {
    console.error('Get messages error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Stream chat response
app.post('/api/chat', async (req, res) => {
  try {
    const { user_id, chat_id, username, content } = req.body;

    if (!content) {
      return res.status(400).json({ error: 'Content required' });
    }

    // Save user message
    await dbRun(
      'INSERT INTO messages (chat_id, user_id, role, content) VALUES (?, ?, ?, ?)',
      [chat_id, user_id, 'user', content]
    );

    // Get chat history
    const messages = await dbAll(
      'SELECT role, content FROM messages WHERE chat_id = ? ORDER BY created_at ASC',
      [chat_id]
    );

    // Build OpenRouter request
    const openrouterMessages = messages.map(msg => ({
      role: msg.role,
      content: msg.content
    }));

    // Set response headers for streaming
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    try {
      const response = await axios.post(
        OPENROUTER_BASE_URL,
        {
          model: MODEL,
          messages: [
            { role: 'system', content: SYSTEM_PROMPT },
            ...openrouterMessages
          ],
          temperature: 0.7,
          max_tokens: 2000,
          stream: true
        },
        {
          headers: {
            'Authorization': `Bearer ${OPENROUTER_API_KEY}`,
            'HTTP-Referer': 'https://mini-ai.app',
            'X-Title': 'Mini AI'
          },
          responseType: 'stream'
        }
      );

      let fullResponse = '';

      response.data.on('data', (chunk) => {
        const lines = chunk.toString().split('\n');
        
        lines.forEach(line => {
          if (line.startsWith('data: ')) {
            try {
              const data = JSON.parse(line.slice(6));
              const content = data.choices?.[0]?.delta?.content || '';
              if (content) {
                fullResponse += content;
                res.write(`data: ${JSON.stringify({ content })}\n\n`);
              }
            } catch (e) {
              // Ignore parse errors
            }
          }
        });
      });

      response.data.on('end', async () => {
        // Save assistant message
        if (fullResponse) {
          await dbRun(
            'INSERT INTO messages (chat_id, user_id, role, content) VALUES (?, ?, ?, ?)',
            [chat_id, user_id, 'assistant', fullResponse]
          );
        }
        res.end();
      });

      response.data.on('error', (error) => {
        console.error('Stream error:', error);
        res.end();
      });

    } catch (apiError) {
      console.error('OpenRouter API error:', apiError.response?.data || apiError.message);
      res.write(`data: ${JSON.stringify({ error: 'API Error' })}\n\n`);
      res.end();
    }

  } catch (error) {
    console.error('Chat error:', error);
    res.status(500).json({ error: error.message });
  }
});

// File upload
app.post('/api/upload', async (req, res) => {
  try {
    const { chat_id } = req.body;
    const file = req.files?.file;

    if (!file || !chat_id) {
      return res.status(400).json({ error: 'File and chat_id required' });
    }

    // For now, just acknowledge upload
    res.json({ 
      success: true, 
      filename: file.name,
      size: file.size 
    });

  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Admin panel endpoint
app.get('/admin/stats', (req, res) => {
  const { secret } = req.query;
  if (secret !== ADMIN_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  Promise.all([
    dbGet('SELECT COUNT(*) as count FROM users'),
    dbGet('SELECT COUNT(*) as count FROM chats'),
    dbGet('SELECT COUNT(*) as count FROM messages')
  ]).then(([users, chats, messages]) => {
    res.json({
      users: users?.count || 0,
      chats: chats?.count || 0,
      messages: messages?.count || 0
    });
  }).catch(error => {
    res.status(500).json({ error: error.message });
  });
});

// Serve admin panel
app.get('/admin', (req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>Mini AI Admin</title>
      <style>
        body { font-family: Arial; background: #1a1a1a; color: #fff; padding: 20px; }
        .container { max-width: 600px; margin: 0 auto; }
        h1 { color: #4a9eff; }
        .stat { background: #2a2a2a; padding: 15px; margin: 10px 0; border-radius: 8px; }
        .stat-value { font-size: 24px; font-weight: bold; color: #4a9eff; }
      </style>
    </head>
    <body>
      <div class="container">
        <h1>Mini AI Admin Panel</h1>
        <div class="stat">
          <div>Total Users</div>
          <div class="stat-value" id="users">-</div>
        </div>
        <div class="stat">
          <div>Total Chats</div>
          <div class="stat-value" id="chats">-</div>
        </div>
        <div class="stat">
          <div>Total Messages</div>
          <div class="stat-value" id="messages">-</div>
        </div>
      </div>
      <script>
        const secret = prompt('Enter admin secret:');
        fetch('/admin/stats?secret=' + secret)
          .then(r => r.json())
          .then(data => {
            document.getElementById('users').textContent = data.users || 0;
            document.getElementById('chats').textContent = data.chats || 0;
            document.getElementById('messages').textContent = data.messages || 0;
          })
          .catch(() => alert('Unauthorized'));
      </script>
    </body>
    </html>
  `);
});

// Start server
app.listen(PORT, () => {
  console.log(`✅ Mini AI Backend running on port ${PORT}`);
  console.log(`📡 OpenRouter Model: ${MODEL}`);
  console.log(`🔑 API Key configured: ${OPENROUTER_API_KEY ? 'Yes' : 'No'}`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  db.close();
  process.exit(0);
});
