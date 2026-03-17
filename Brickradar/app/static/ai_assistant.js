// BrickRadar AI Assistant — floating chat widget
(function() {
  const cfg = window.AI_PAGE_CONFIG || {};

  const FAQS = cfg.faqs || [
    "What is on my RadarList?",
    "Any price drops today?",
    "Best deals right now?",
    "Cheapest store overall?",
    "Cheapest Technic sets?",
    "What is new since last refresh?",
  ];

  const PLACEHOLDER = cfg.placeholder || "Ask anything about LEGO prices…";
  const PAGE_HINT   = cfg.pageHint   || null;

  const style = document.createElement('style');
  style.textContent = `
    #brAIBtn {
      position:fixed;bottom:1.5rem;right:1.5rem;width:52px;height:52px;border-radius:50%;
      background:linear-gradient(135deg,var(--acc,#6366f1),var(--acc2,#8b5cf6));
      border:none;cursor:pointer;box-shadow:0 4px 20px rgba(99,102,241,0.4);
      display:flex;align-items:center;justify-content:center;font-size:1.3rem;
      z-index:9999;transition:all 0.2s;color:#fff;
    }
    #brAIBtn:hover { transform:scale(1.1); box-shadow:0 6px 28px rgba(99,102,241,0.6); }
    #brAIPanel {
      position:fixed;bottom:5rem;right:1.5rem;width:370px;max-height:560px;
      background:var(--surf,#0d1420);border:1px solid var(--bord2,rgba(99,102,241,0.35));
      border-radius:16px;box-shadow:0 20px 60px rgba(0,0,0,0.5);
      display:none;flex-direction:column;z-index:9998;overflow:hidden;
      font-family:'DM Sans',system-ui,sans-serif;
    }
    #brAIPanel.open { display:flex; animation:slideUp 0.2s ease; }
    @keyframes slideUp { from{opacity:0;transform:translateY(12px)} to{opacity:1;transform:translateY(0)} }
    #brAIHeader {
      padding:.65rem 1rem;
      background:linear-gradient(135deg,rgba(99,102,241,0.15),rgba(139,92,246,0.1));
      border-bottom:1px solid var(--bord,rgba(99,102,241,0.13));
      display:flex;align-items:center;justify-content:space-between;flex-shrink:0;
    }
    #brAIHeader .title { font-weight:700;font-size:.85rem;color:var(--text,#e2e8f0);display:flex;align-items:center;gap:.4rem; }
    .brAI-hdr-actions { display:flex;align-items:center;gap:.3rem; }
    .brAI-hdr-btn { background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);border-radius:6px;cursor:pointer;color:var(--muted,#475569);font-size:.7rem;padding:.2rem .45rem;transition:all .15s; }
    .brAI-hdr-btn:hover { color:var(--text,#e2e8f0);background:rgba(255,255,255,0.1); }
    #brAIMsgs {
      flex:1;overflow-y:auto;padding:.75rem;display:flex;flex-direction:column;gap:.5rem;
      scrollbar-width:thin;scrollbar-color:rgba(99,102,241,0.3) transparent;
    }
    .brAI-msg { max-width:88%;line-height:1.5;font-size:.78rem;padding:.55rem .75rem;border-radius:10px;white-space:pre-wrap; }
    .brAI-msg.user { align-self:flex-end;background:var(--acc,#6366f1);color:#fff;border-bottom-right-radius:3px; }
    .brAI-msg.ai { align-self:flex-start;background:rgba(255,255,255,0.05);color:var(--text,#e2e8f0);border:1px solid var(--bord,rgba(99,102,241,0.13));border-bottom-left-radius:3px; }
    .brAI-msg.ai.streaming::after { content:'▊';animation:blink .6s infinite;color:var(--acc,#6366f1); }
    @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0} }
    #brAIFAQ {
      padding:.5rem .75rem;border-top:1px solid var(--bord,rgba(99,102,241,0.13));flex-shrink:0;
      background:rgba(0,0,0,0.15);
    }
    .brAI-faq-label { font-size:.58rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--muted,#475569);margin-bottom:.35rem; }
    .brAI-faq-grid { display:flex;flex-wrap:wrap;gap:.3rem; }
    .brAI-sug {
      background:rgba(99,102,241,0.07);border:1px solid rgba(99,102,241,0.18);
      border-radius:20px;padding:.2rem .55rem;font-size:.64rem;cursor:pointer;
      color:#a5b4fc;white-space:nowrap;transition:all .15s;
    }
    .brAI-sug:hover { background:rgba(99,102,241,0.2);color:#fff;border-color:rgba(99,102,241,0.4); }
    #brAIInputRow { padding:.55rem .75rem;display:flex;gap:.4rem;border-top:1px solid var(--bord,rgba(99,102,241,0.13));flex-shrink:0; }
    #brAIInput {
      flex:1;background:rgba(255,255,255,0.05);border:1px solid var(--bord,rgba(99,102,241,0.13));
      border-radius:8px;padding:.4rem .65rem;color:var(--text,#e2e8f0);font-size:.78rem;
      outline:none;font-family:inherit;
    }
    #brAIInput:focus { border-color:var(--acc,#6366f1); }
    #brAISend {
      background:var(--acc,#6366f1);border:none;border-radius:8px;
      padding:.4rem .75rem;cursor:pointer;color:#fff;font-size:.85rem;transition:all .15s;
    }
    #brAISend:hover { opacity:.85; }
    #brAISend:disabled { opacity:.4;cursor:default; }
  `;
  document.head.appendChild(style);

  const btn = document.createElement('button');
  btn.id = 'brAIBtn';
  btn.title = 'BrickRadar AI';
  btn.innerHTML = '✦';

  const panel = document.createElement('div');
  panel.id = 'brAIPanel';
  panel.innerHTML = `
    <div id="brAIHeader">
      <div class="title">
        ✦ BrickRadar AI
        ${PAGE_HINT ? `<span style="font-size:.6rem;color:#64748b;font-weight:400">${PAGE_HINT}</span>` : ''}
        <span style="font-size:.6rem;background:rgba(99,102,241,0.2);color:#a5b4fc;border-radius:4px;padding:.1rem .35rem;font-weight:600">BETA</span>
      </div>
      <div class="brAI-hdr-actions">
        <button class="brAI-hdr-btn" onclick="clearAIChat()" title="Clear conversation">🗑 Clear</button>
        <button class="brAI-hdr-btn" onclick="toggleAI()">✕</button>
      </div>
    </div>
    <div id="brAIMsgs">
      <div class="brAI-msg ai">Hi! I'm your BrickRadar AI. Ask me about prices, deals, or your RadarList. 🎯</div>
    </div>
    <div id="brAIFAQ">
      <div class="brAI-faq-label">Quick questions</div>
      <div class="brAI-faq-grid" id="brAIChips">
        ${FAQS.map(s => `<span class="brAI-sug" data-q="${s.replace(/"/g,'&quot;')}">${s}</span>`).join('')}
      </div>
    </div>
    <div id="brAIInputRow">
      <input id="brAIInput" placeholder="${PLACEHOLDER}" onkeydown="if(event.key==='Enter'&&!event.shiftKey)sendAI()">
      <button id="brAISend" onclick="sendAI()">➤</button>
    </div>
  `;

  document.body.appendChild(btn);
  document.body.appendChild(panel);

  // Allow pages to push dynamic chips after data loads
  window.AI_UPDATE_CHIPS = function(questions) {
    const chips = document.getElementById('brAIChips');
    if (!chips || !questions || !questions.length) return;
    chips.innerHTML = questions
      .map(s => `<span class="brAI-sug" data-q="${s.replace(/"/g,'&quot;')}">${s}</span>`)
      .join('');
  };

  let messages = [];

  window.toggleAI = function() {
    panel.classList.toggle('open');
    if (panel.classList.contains('open')) document.getElementById('brAIInput').focus();
  };
  btn.onclick = toggleAI;

  window.clearAIChat = function() {
    messages = [];
    const msgsEl = document.getElementById('brAIMsgs');
    msgsEl.innerHTML = '<div class="brAI-msg ai">Chat cleared. Ask me anything! 🎯</div>';
  };

  window.askAI = function(text) {
    document.getElementById('brAIInput').value = text;
    sendAI();
  };

  // Delegated handler for suggestion chips
  document.addEventListener('click', function(e) {
    const chip = e.target.closest('.brAI-sug');
    if (chip && chip.dataset.q) askAI(chip.dataset.q);
  });

  window.sendAI = async function() {
    const input = document.getElementById('brAIInput');
    const text = input.value.trim();
    if (!text) return;

    const send = document.getElementById('brAISend');
    const msgsEl = document.getElementById('brAIMsgs');

    messages.push({role: 'user', content: text});
    input.value = '';
    send.disabled = true;

    const userEl = document.createElement('div');
    userEl.className = 'brAI-msg user';
    userEl.textContent = text;
    msgsEl.appendChild(userEl);

    const aiEl = document.createElement('div');
    aiEl.className = 'brAI-msg ai streaming';
    aiEl.textContent = '';
    msgsEl.appendChild(aiEl);
    msgsEl.scrollTop = msgsEl.scrollHeight;

    try {
      const resp = await fetch('/api/ai/chat', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({messages})
      });

      if (!resp.ok) {
        const err = await resp.json();
        aiEl.textContent = '⚠ ' + (err.error || 'Something went wrong');
        aiEl.classList.remove('streaming');
        send.disabled = false;
        return;
      }

      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let fullText = '';

      while (true) {
        const {done, value} = await reader.read();
        if (done) break;
        const chunk = decoder.decode(value);
        for (const line of chunk.split('\n')) {
          if (line.startsWith('data: ')) {
            const data = line.slice(6);
            if (data === '[DONE]') break;
            try {
              const evt = JSON.parse(data);
              if (evt.text) {
                fullText += evt.text;
                aiEl.textContent = fullText;
                msgsEl.scrollTop = msgsEl.scrollHeight;
              }
            } catch(e) {}
          }
        }
      }

      aiEl.classList.remove('streaming');
      messages.push({role: 'assistant', content: fullText});

    } catch(e) {
      aiEl.textContent = '⚠ Connection error.';
      aiEl.classList.remove('streaming');
    }

    send.disabled = false;
    msgsEl.scrollTop = msgsEl.scrollHeight;
  };
})();
