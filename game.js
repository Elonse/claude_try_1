(() => {
  'use strict';

  // ---------- DOM ----------
  const canvas = document.getElementById('game');
  const ctx = canvas.getContext('2d');
  const scoreEl = document.getElementById('score');
  const highScoreEl = document.getElementById('high-score');
  const finalScoreEl = document.getElementById('final-score');
  const startBtn = document.getElementById('start-btn');
  const restartBtn = document.getElementById('restart-btn');
  const overlayEl = document.getElementById('overlay');
  const overlayTitle = document.getElementById('overlay-title');
  const difficultyEl = document.getElementById('difficulty');
  const dpadBtns = document.querySelectorAll('.dpad-btn');

  // ---------- Constants ----------
  const GRID = 20; // 20x20 cells
  const DIFFICULTY = {
    easy: 160,
    normal: 110,
    hard: 70,
  };
  const STORAGE_KEY = 'snakeHighScore';

  // ---------- State ----------
  let snake;
  let direction;
  let pendingDirection;
  let food;
  let score;
  let highScore;
  let isRunning = false;
  let isPaused = false;
  let tickMs = DIFFICULTY.normal;
  let loopId = null;

  // ---------- Init ----------
  highScore = parseInt(localStorage.getItem(STORAGE_KEY) || '0', 10) || 0;
  highScoreEl.textContent = highScore;

  function reset() {
    snake = [
      { x: 10, y: 10 },
      { x: 9, y: 10 },
      { x: 8, y: 10 },
    ];
    direction = { x: 1, y: 0 };
    pendingDirection = { x: 1, y: 0 };
    score = 0;
    scoreEl.textContent = '0';
    placeFood();
  }

  function placeFood() {
    while (true) {
      const f = {
        x: Math.floor(Math.random() * GRID),
        y: Math.floor(Math.random() * GRID),
      };
      if (!snake.some((s) => s.x === f.x && s.y === f.y)) {
        food = f;
        return;
      }
    }
  }

  // ---------- Rendering ----------
  function draw() {
    const w = canvas.width;
    const h = canvas.height;
    const cell = w / GRID;

    // Background
    ctx.fillStyle = '#fafcfb';
    ctx.fillRect(0, 0, w, h);

    // Subtle checker pattern for depth
    ctx.fillStyle = 'rgba(168, 213, 186, 0.08)';
    for (let y = 0; y < GRID; y++) {
      for (let x = 0; x < GRID; x++) {
        if ((x + y) % 2 === 0) {
          ctx.fillRect(x * cell, y * cell, cell, cell);
        }
      }
    }

    // Food (circle with soft glow)
    const fx = food.x * cell + cell / 2;
    const fy = food.y * cell + cell / 2;
    const fr = cell / 2 - 2;
    const grad = ctx.createRadialGradient(fx, fy, 0, fx, fy, fr * 1.4);
    grad.addColorStop(0, '#ffb3c6');
    grad.addColorStop(1, '#ff8fab');
    ctx.fillStyle = grad;
    ctx.beginPath();
    ctx.arc(fx, fy, fr, 0, Math.PI * 2);
    ctx.fill();

    // Snake (rounded rects)
    snake.forEach((segment, idx) => {
      const isHead = idx === 0;
      ctx.fillStyle = isHead ? '#4a9b6f' : '#7bbf95';
      const pad = isHead ? 1 : 2;
      const r = (cell - pad * 2) * 0.3;
      roundRect(
        ctx,
        segment.x * cell + pad,
        segment.y * cell + pad,
        cell - pad * 2,
        cell - pad * 2,
        r
      );
      ctx.fill();
    });
  }

  function roundRect(ctx, x, y, w, h, r) {
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.arcTo(x + w, y, x + w, y + h, r);
    ctx.arcTo(x + w, y + h, x, y + h, r);
    ctx.arcTo(x, y + h, x, y, r);
    ctx.arcTo(x, y, x + w, y, r);
    ctx.closePath();
  }

  // ---------- Game loop ----------
  function tick() {
    if (isPaused) return;

    direction = pendingDirection;
    const head = {
      x: snake[0].x + direction.x,
      y: snake[0].y + direction.y,
    };

    // Wall collision
    if (head.x < 0 || head.x >= GRID || head.y < 0 || head.y >= GRID) {
      return endGame();
    }

    // Self collision
    if (snake.some((s) => s.x === head.x && s.y === head.y)) {
      return endGame();
    }

    snake.unshift(head);

    if (head.x === food.x && head.y === food.y) {
      score += 10;
      scoreEl.textContent = String(score);
      placeFood();
    } else {
      snake.pop();
    }

    draw();
  }

  function startLoop() {
    stopLoop();
    loopId = setInterval(tick, tickMs);
  }

  function stopLoop() {
    if (loopId !== null) {
      clearInterval(loopId);
      loopId = null;
    }
  }

  // ---------- Flow ----------
  function startGame() {
    reset();
    tickMs = DIFFICULTY[difficultyEl.value] || DIFFICULTY.normal;
    isRunning = true;
    isPaused = false;
    overlayEl.classList.add('hidden');
    startBtn.textContent = 'איפוס';
    draw();
    startLoop();
  }

  function endGame() {
    stopLoop();
    isRunning = false;
    if (score > highScore) {
      highScore = score;
      localStorage.setItem(STORAGE_KEY, String(highScore));
      highScoreEl.textContent = String(highScore);
      overlayTitle.textContent = 'שיא חדש!';
    } else {
      overlayTitle.textContent = 'המשחק נגמר';
    }
    finalScoreEl.textContent = String(score);
    overlayEl.classList.remove('hidden');
    startBtn.textContent = 'התחל';
  }

  // ---------- Input ----------
  function setDirection(dx, dy) {
    // Prevent 180° reversal based on the CURRENT committed direction
    if (dx === -direction.x && dy === -direction.y) return;
    // Also prevent conflicting double-sets within the same tick
    if (dx === direction.x && dy === direction.y) return;
    pendingDirection = { x: dx, y: dy };
  }

  const KEY_MAP = {
    ArrowUp: [0, -1],
    ArrowDown: [0, 1],
    ArrowLeft: [-1, 0],
    ArrowRight: [1, 0],
    w: [0, -1], W: [0, -1],
    s: [0, 1],  S: [0, 1],
    a: [-1, 0], A: [-1, 0],
    d: [1, 0],  D: [1, 0],
  };

  function handleKey(e) {
    const m = KEY_MAP[e.key];
    if (!m) return;
    e.preventDefault();
    if (!isRunning) return;
    setDirection(m[0], m[1]);
  }

  // Touch swipe
  let touchStart = null;
  const SWIPE_THRESHOLD = 25;

  function handleTouchStart(e) {
    if (e.touches.length !== 1) return;
    const t = e.touches[0];
    touchStart = { x: t.clientX, y: t.clientY };
  }

  function handleTouchEnd(e) {
    if (!touchStart) return;
    const t = e.changedTouches[0];
    const dx = t.clientX - touchStart.x;
    const dy = t.clientY - touchStart.y;
    touchStart = null;
    if (Math.abs(dx) < SWIPE_THRESHOLD && Math.abs(dy) < SWIPE_THRESHOLD) return;
    if (!isRunning) return;
    if (Math.abs(dx) > Math.abs(dy)) {
      setDirection(dx > 0 ? 1 : -1, 0);
    } else {
      setDirection(0, dy > 0 ? 1 : -1);
    }
  }

  // D-pad
  const DPAD_MAP = {
    up: [0, -1],
    down: [0, 1],
    left: [-1, 0],
    right: [1, 0],
  };

  function handleDpad(e) {
    const dir = e.currentTarget.dataset.dir;
    const m = DPAD_MAP[dir];
    if (!m) return;
    e.preventDefault();
    if (!isRunning) {
      startGame();
      setDirection(m[0], m[1]);
      return;
    }
    setDirection(m[0], m[1]);
  }

  // ---------- Wire up ----------
  startBtn.addEventListener('click', startGame);
  restartBtn.addEventListener('click', startGame);
  window.addEventListener('keydown', handleKey);
  canvas.addEventListener('touchstart', handleTouchStart, { passive: true });
  canvas.addEventListener('touchend', handleTouchEnd, { passive: true });
  dpadBtns.forEach((btn) => {
    btn.addEventListener('click', handleDpad);
  });

  difficultyEl.addEventListener('change', () => {
    tickMs = DIFFICULTY[difficultyEl.value] || DIFFICULTY.normal;
    if (isRunning && !isPaused) startLoop();
  });

  // Pause when tab hidden
  document.addEventListener('visibilitychange', () => {
    if (document.hidden && isRunning) {
      isPaused = true;
      stopLoop();
    } else if (!document.hidden && isRunning && isPaused) {
      isPaused = false;
      startLoop();
    }
  });

  // Initial render
  reset();
  draw();
})();
