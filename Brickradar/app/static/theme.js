// BrickRadar Theme System
const THEMES = {
  dark: {
    name: 'Dark', icon: '🌑',
    '--bg': '#080c14', '--surf': '#0d1420', '--surf2': '#111827',
    '--bord': 'rgba(99,102,241,0.13)', '--bord2': 'rgba(99,102,241,0.35)',
    '--text': '#e2e8f0', '--muted': '#475569', '--acc': '#6366f1',
    '--acc2': '#8b5cf6', '--success': '#10b981', '--warn': '#f59e0b', '--danger': '#ef4444',
    '--tw-bg': '#0f172a', '--tw-surf': '#1e293b', '--tw-bord': '#334155',
    '--tw-text': '#e2e8f0', '--tw-muted': '#94a3b8', '--tw-acc': '#6366f1',
  },
  midnight: {
    name: 'Midnight', icon: '🌌',
    '--bg': '#000000', '--surf': '#0a0a0f', '--surf2': '#111118',
    '--bord': 'rgba(6,182,212,0.15)', '--bord2': 'rgba(6,182,212,0.4)',
    '--text': '#e0f2fe', '--muted': '#334155', '--acc': '#06b6d4',
    '--acc2': '#0ea5e9', '--success': '#10b981', '--warn': '#f59e0b', '--danger': '#ef4444',
    '--tw-bg': '#000000', '--tw-surf': '#0f0f1a', '--tw-bord': '#1e2533',
    '--tw-text': '#e0f2fe', '--tw-muted': '#64748b', '--tw-acc': '#06b6d4',
  },
  slate: {
    name: 'Slate', icon: '🩶',
    '--bg': '#0f172a', '--surf': '#1e293b', '--surf2': '#263548',
    '--bord': 'rgba(148,163,184,0.15)', '--bord2': 'rgba(148,163,184,0.35)',
    '--text': '#f1f5f9', '--muted': '#64748b', '--acc': '#3b82f6',
    '--acc2': '#60a5fa', '--success': '#10b981', '--warn': '#f59e0b', '--danger': '#ef4444',
    '--tw-bg': '#0f172a', '--tw-surf': '#1e293b', '--tw-bord': '#334155',
    '--tw-text': '#f1f5f9', '--tw-muted': '#94a3b8', '--tw-acc': '#3b82f6',
  },
  ember: {
    name: 'Ember', icon: '🔥',
    '--bg': '#0c0a08', '--surf': '#1a1208', '--surf2': '#231a0e',
    '--bord': 'rgba(245,158,11,0.15)', '--bord2': 'rgba(245,158,11,0.4)',
    '--text': '#fef3c7', '--muted': '#78350f', '--acc': '#f59e0b',
    '--acc2': '#f97316', '--success': '#10b981', '--warn': '#fbbf24', '--danger': '#ef4444',
    '--tw-bg': '#1c1008', '--tw-surf': '#292010', '--tw-bord': '#451a03',
    '--tw-text': '#fef3c7', '--tw-muted': '#92400e', '--tw-acc': '#f59e0b',
  },
  forest: {
    name: 'Forest', icon: '🌲',
    '--bg': '#071a0f', '--surf': '#0d2618', '--surf2': '#123020',
    '--bord': 'rgba(16,185,129,0.15)', '--bord2': 'rgba(16,185,129,0.4)',
    '--text': '#d1fae5', '--muted': '#065f46', '--acc': '#10b981',
    '--acc2': '#34d399', '--success': '#34d399', '--warn': '#f59e0b', '--danger': '#ef4444',
    '--tw-bg': '#071a0f', '--tw-surf': '#0d2618', '--tw-bord': '#064e3b',
    '--tw-text': '#d1fae5', '--tw-muted': '#6ee7b7', '--tw-acc': '#10b981',
  },
  light: {
    name: 'Light', icon: '☀️',
    '--bg': '#f8fafc', '--surf': '#ffffff', '--surf2': '#f1f5f9',
    '--bord': 'rgba(99,102,241,0.15)', '--bord2': 'rgba(99,102,241,0.4)',
    '--text': '#0f172a', '--muted': '#64748b', '--acc': '#6366f1',
    '--acc2': '#8b5cf6', '--success': '#10b981', '--warn': '#f59e0b', '--danger': '#ef4444',
    '--tw-bg': '#f8fafc', '--tw-surf': '#ffffff', '--tw-bord': '#e2e8f0',
    '--tw-text': '#0f172a', '--tw-muted': '#64748b', '--tw-acc': '#6366f1',
  },
};

const CURRENT_THEME_KEY = 'brickradar_theme';

function applyTheme(name) {
  const t = THEMES[name] || THEMES.dark;
  const root = document.documentElement;

  // Apply CSS variables
  Object.entries(t).forEach(([k, v]) => {
    if (k.startsWith('--') && !k.startsWith('--tw-')) root.style.setProperty(k, v);
  });

  // Also set aliases used by different pages
  root.style.setProperty('--border', t['--bord']);
  root.style.setProperty('--border-hover', t['--bord2']);
  root.style.setProperty('--accent', t['--acc']);
  root.style.setProperty('--surface', t['--surf']);

  // Tailwind override via body classes + inline styles
  const body = document.body;
  body.style.background = t['--bg'];
  body.style.color = t['--text'];

  // Override Tailwind slate colors via CSS injection
  let styleEl = document.getElementById('theme-override');
  if (!styleEl) { styleEl = document.createElement('style'); styleEl.id = 'theme-override'; document.head.appendChild(styleEl); }

  const tw = t;
  styleEl.textContent = `
    body, .bg-slate-900 { background-color: ${tw['--tw-bg']} !important; }
    .bg-slate-800, .bg-slate-800\\/40 { background-color: ${tw['--tw-surf']} !important; }
    .bg-slate-700 { background-color: ${tw['--tw-bord']} !important; }
    .border-slate-700, .border-slate-600 { border-color: ${tw['--tw-bord']} !important; }
    .text-slate-400, .text-slate-300, .text-slate-200 { color: ${tw['--tw-muted']} !important; }
    .text-white { color: ${tw['--tw-text']} !important; }
    .bg-indigo-500 { background-color: ${tw['--tw-acc']} !important; }
    .hover\\:bg-indigo-400:hover { background-color: ${tw['--acc2']} !important; }
    .hover\\:bg-slate-700:hover { background-color: ${tw['--tw-bord']} !important; }
    select, input[type="text"], input[type="number"] {
      background: ${tw['--tw-surf']} !important;
      color: ${tw['--tw-text']} !important;
      border-color: ${tw['--tw-bord']} !important;
    }
    thead th { background: ${tw['--tw-surf']} !important; }
    .about-card, #exportMenu, .about-overlay .about-card { background: ${tw['--surf']} !important; }
  `;

  // Update theme switcher active state
  document.querySelectorAll('.theme-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.theme === name);
  });

  localStorage.setItem(CURRENT_THEME_KEY, name);
}

function initTheme() {
  const saved = localStorage.getItem(CURRENT_THEME_KEY) || 'dark';
  applyTheme(saved);
}

function buildThemeSwitcher(containerId) {
  const container = document.getElementById(containerId);
  if (!container) return;
  container.innerHTML = Object.entries(THEMES).map(([key, t]) =>
    `<button class="theme-btn" data-theme="${key}" onclick="applyTheme('${key}')" title="${t.name}"
      style="width:28px;height:28px;border-radius:50%;border:2px solid transparent;cursor:pointer;font-size:14px;background:none;transition:all 0.15s;display:flex;align-items:center;justify-content:center;"
      onmouseover="this.style.transform='scale(1.2)'" onmouseout="this.style.transform='scale(1)'">${t.icon}</button>`
  ).join('');
}

// Auto-init on load
document.addEventListener('DOMContentLoaded', () => {
  initTheme();
  buildThemeSwitcher('themeSwitcher');
});
