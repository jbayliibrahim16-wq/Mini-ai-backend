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
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.static(__dirname)); // << جديد

// Database setup
const dbPath = path.join(__dirname, 'database.sqlite');
const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.error('Database connection error:', err);
  } else {
    console.log('Connected to SQLite database');
    initDatabase();
  }
});

// Initialize database tables
function initDatabase() {
  db.run(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS chats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    title TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(user_id) REFERENCES users(id)
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(chat_id) REFERENCES chats(id)
  )`);
}

// Helper functions
function dbGet(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

function dbAll(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows || []);
    });
  });
}

function dbRun(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function(err) {
      if (err) reject(err);
      else resolve({ id: this.lastID, changes: this.changes });
    });
  });
}

// << جديد: الواجهة الرئيسية
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Health check
app.get('/api/status', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Create or get user
app.post('/api/users', async (req, res) => {
  try {
    const { username } = req.body;
    if (!username) {
      return res.status(400).json({ error: 'Username required' });
    }

    let user = await dbGet('SELECT * FROM users WHERE username =?', [username]);
    if (!user) {
      const result = await dbRun('INSERT INTO users (username) VALUES (?)', [username]);
      user = { id: result.id, username: username };
    }
    res.json(user);
  } catch (error) {
    console.error('Create user error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Create new chat
app.post('/api/chats', async (req, res) => {
  try {
    const { user_id, title = 'New Chat' } = req.body;
    const result = await dbRun('INSERT INTO chats (user_id, title) VALUES (?,?)', [user_id, title]);
    res.json({ id: result.id, user_id: user_id, title: title });
  } catch (error) {
    console.error('Create chat error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get chat messages
app.get('/api/chats/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const messages = await dbAll('SELECT * FROM messages WHERE chat_id =? ORDER BY created_at ASC', [id]);
    res.json(messages);
  } catch (error) {
    console.error('Get chat error:', error);
    res.status(500).json({ error: error.message });
  }
});

// << جديد: API الشات للواجهة - عدل هون وحط كود OpenRouter تبعك
app.post('/api/chat', async (req, res) => {
  try {
    const { message, history = [] } = req.body;
    const response = await axios.post('https://openrouter.ai/api/v1/chat/completions', {
      model: 'openai/gpt-3.5-turbo',
      messages: [...history, { role: 'user', content: message }]
    }, {
      headers: {
        'Authorization': `Bearer ${process.env.OPENROUTER_KEY}`,
        'Content-Type': 'application/json'
      }
    });
    const reply = response.data.choices[0].message.content;
    res.json({ reply: reply });
  } catch (error) {
    console.error('Chat Error:', error.response?.data || error.message);
    res.status(500).json({ error: 'صار خطأ بالسيرفر' });
  }
});

// Stream chat response
app.post('/api/chat/stream', async (req, res) => {
  try {
    const { chat_id, message } = req.body;

    // Set headers for streaming
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    // Save user message
    await dbRun('INSERT INTO messages (chat_id, role, content) VALUES (?,?,?)', [chat_id, 'user', message]);

    // Get chat history
    const messages = await dbAll('SELECT role, content FROM messages WHERE chat_id =? ORDER BY created_at ASC', [chat_id]);

    try {
      const response = await axios.post('https://openrouter.ai/api/v1/chat/completions', {
        model: 'openai/gpt-3.5-turbo',
        messages: messages.map(msg => ({ role: msg.role, content: msg.content })),
        stream: true
      }, {
        headers: {
          'Authorization': `Bearer ${process.env.OPENROUTER_KEY}`,
          'Content-Type': 'application/json'
        },
        responseType: 'stream'
      });

      let fullResponse = '';

      response.data.on('data', (chunk) => {
        const lines = chunk.toString().split('\n');

        for (const line of lines) {
          if (line.startsWith('data: ')) {
            const data = line.slice(6);
            if (data === '[DONE]') {
              // Save full response to database
              dbRun('INSERT INTO messages (chat_id, role, content) VALUES (?,?,?)', [chat_id, 'assistant', fullResponse]);
              res.write('data: [DONE]\n\n');
              res.end();
              return;
            }

            try {
              const parsed = JSON.parse(data);
              const content = parsed.choices[0]?.delta?.content || '';
              if (content) {
                fullResponse += content;
                res.write(`data: ${JSON.stringify({ content })}\n\n`);
              }
            } catch (e) {
              // Skip parse errors
            }
          }
        }
      });

      response.data.on('error', (error) => {
        console.error('Stream error:', error);
        res.write(`data: ${JSON.stringify({ error: 'Stream error' })}\n\n`);
        res.end();
      });

    } catch (apiError) {
      console.error('OpenRouter API error:', apiError.response?.data || apiError.message);
      res.write(`data: ${JSON.stringify({ error: 'AI service error' })}\n\n`);
      res.end();
    }

  } catch (error) {
    console.error('Chat stream error:', error);
    res.status(500).json({ error: error.message });
  }
});

// File upload
app.post('/api/upload', async (req, res) => {
  try {
    const { chat_id } = req.body;
    // For now, just acknowledge upload
    res.json({ success: true, message: 'File upload not implemented yet' });
  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Admin stats endpoint
app.get('/admin/stats', async (req, res) => {
  try {
    const { secret } = req.query;
    if (secret!== process.env.ADMIN_SECRET) {
      return res.status(403).json({ error: 'Unauthorized' });
    }

    const users = await dbGet('SELECT COUNT(*) as count FROM users');
    const chats = await dbGet('SELECT COUNT(*) as count FROM chats');
    const messages = await dbGet('SELECT COUNT(*) as count FROM messages');

    res.json({
      users: users.count || 0,
      chats: chats.count || 0,
      messages: messages.count || 0
    });
  } catch (error) {
    console.error('Admin stats error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Admin panel HTML
app.get('/admin', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Admin Panel</title>
    <style>
        body { font-family: Arial; background: #1a1a1a; color: #fff; margin: 0; padding: 20px; }
       .container { max-width: 600px; margin: 0 auto; }
       .stat-box { background: #2a2a2a; padding: 15px; margin: 10px 0; border-radius: 8px; }
       .stat-value { font-size: 24px; font-weight: bold; }
        h1 { text-align: center; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Admin Panel</h1>
        <div class="stat-box">
            <div>Total Users</div>
            <div class="stat-value" id="users">-</div>
        </div>
        <div class="stat-box">
            <div>Total Chats</div>
            <div class="stat-value" id="chats">-</div>
        </div>
        <div class="stat-box">
            <div>Total Messages</div>
            <div class="stat-value" id="messages">-</div>
        </div>
    </div>
    <script>
        const secret = prompt("Enter admin secret:");
        fetch('/admin/stats?secret=' + secret)
           .then(r => r.json())
           .then(data => {
                document.getElementById('users').textContent = data.users;
                document.getElementById('chats').textContent = data.chats;
                document.getElementById('messages').textContent = data.messages;
            })
           .catch(e => alert("Unauthorized"));
    </script>
</body>
</html>
  `);
});

app.listen(PORT, () => {
  console.log(`✅ Mini AI Backend running on port ${PORT}`);
  console.log(`🤖 OpenRouter Key configured: ${process.env.OPENROUTER_KEY? 'Yes' : 'No'}`);
  console.log(`🔑 Admin Key configured: ${process.env.ADMIN_SECRET? 'Yes' : 'No'}`);
});
