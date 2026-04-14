(() => {
  const canvas = document.getElementById('game');
  const ctx = canvas.getContext('2d');
  const scoreEl = document.getElementById('score');
  const highScoreEl = document.getElementById('high-score');
  const finalScoreEl = document.getElementById('final-score');
  const startBtn = document.getElementById('start-btn');
  const restartBtn = document.getElementById('restart-btn');
  const gameOverEl = document.getElementById('game-over');

  const GRID_SIZE = 20;
  const TILE_COUNT = canvas.width / GRID_SIZE;
  const TICK_MS = 120;

  let snake, direction, pendingDirection, food, score, highScore, loopId, running;

  highScore = parseInt(localStorage.getItem('snakeHighScore') || '0', 10);
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
    scoreEl.textContent = score;
    placeFood();
  }

  function placeFood() {
    while (true) {
      const f = {
        x: Math.floor(Math.random() * TILE_COUNT),
        y: Math.floor(Math.random() * TILE_COUNT),
      };
      if (!snake.some((s) => s.x === f.x && s.y === f.y)) {
        food = f;
        return;
      }
    }
  }

  function draw() {
    // Background
    ctx.fillStyle = '#0f172a';
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    // Grid
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.04)';
    for (let i = 0; i < TILE_COUNT; i++) {
      ctx.beginPath();
      ctx.moveTo(i * GRID_SIZE, 0);
      ctx.lineTo(i * GRID_SIZE, canvas.height);
      ctx.stroke();
      ctx.beginPath();
      ctx.moveTo(0, i * GRID_SIZE);
      ctx.lineTo(canvas.width, i * GRID_SIZE);
      ctx.stroke();
    }

    // Food
    ctx.fillStyle = '#ef4444';
    ctx.beginPath();
    ctx.arc(
      food.x * GRID_SIZE + GRID_SIZE / 2,
      food.y * GRID_SIZE + GRID_SIZE / 2,
      GRID_SIZE / 2 - 2,
      0,
      Math.PI * 2
    );
    ctx.fill();

    // Snake
    snake.forEach((segment, idx) => {
      ctx.fillStyle = idx === 0 ? '#4ade80' : '#22c55e';
      ctx.fillRect(
        segment.x * GRID_SIZE + 1,
        segment.y * GRID_SIZE + 1,
        GRID_SIZE - 2,
        GRID_SIZE - 2
      );
    });
  }

  function step() {
    direction = pendingDirection;
    const head = {
      x: snake[0].x + direction.x,
      y: snake[0].y + direction.y,
    };

    // Wall collision
    if (
      head.x < 0 ||
      head.x >= TILE_COUNT ||
      head.y < 0 ||
      head.y >= TILE_COUNT
    ) {
      return endGame();
    }

    // Self collision
    if (snake.some((s) => s.x === head.x && s.y === head.y)) {
      return endGame();
    }

    snake.unshift(head);

    if (head.x === food.x && head.y === food.y) {
      score += 10;
      scoreEl.textContent = score;
      placeFood();
    } else {
      snake.pop();
    }

    draw();
  }

  function endGame() {
    running = false;
    clearInterval(loopId);
    if (score > highScore) {
      highScore = score;
      localStorage.setItem('snakeHighScore', String(highScore));
      highScoreEl.textContent = highScore;
    }
    finalScoreEl.textContent = score;
    gameOverEl.classList.remove('hidden');
  }

  function startGame() {
    reset();
    draw();
    gameOverEl.classList.add('hidden');
    startBtn.textContent = 'משחק פעיל...';
    startBtn.disabled = true;
    running = true;
    clearInterval(loopId);
    loopId = setInterval(step, TICK_MS);
  }

  function handleKey(e) {
    const key = e.key;
    // RTL: right arrow decreases x, left arrow increases x
    const map = {
      ArrowUp: { x: 0, y: -1 },
      ArrowDown: { x: 0, y: 1 },
      ArrowLeft: { x: -1, y: 0 },
      ArrowRight: { x: 1, y: 0 },
    };
    const next = map[key];
    if (!next) return;
    e.preventDefault();
    // Prevent 180 reversal
    if (next.x === -direction.x && next.y === -direction.y) return;
    pendingDirection = next;
  }

  startBtn.addEventListener('click', startGame);
  restartBtn.addEventListener('click', startGame);
  window.addEventListener('keydown', handleKey);

  // Initial draw
  reset();
  draw();
})();
