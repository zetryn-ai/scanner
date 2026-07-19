module.exports = {
  apps: [
    {
      name: 'zetryn-scanner',
      script: '.venv/bin/python',
      args: '-m scanner.main',
      cwd: __dirname,
      env: {
        REDIS_URL: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
      },
      instances: 1,
      autorestart: true,
    },
    {
      name: 'zetryn-scanner-filter',
      script: '.venv/bin/python',
      args: '-m scanner.filter_main',
      cwd: __dirname,
      env: {
        REDIS_URL: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
        STRATEGIES_CONFIG_PATH: process.env.STRATEGIES_CONFIG_PATH || './strategies.json',
      },
      instances: 1,
      autorestart: true,
    },
  ],
}
