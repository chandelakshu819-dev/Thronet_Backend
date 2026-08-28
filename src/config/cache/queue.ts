import 'dotenv/config';

export const queueConfig: any = {
  bull: {
    redis: {
      host: process.env.BULL_REDIS_HOST || process.env.REDIS_HOST || 'localhost',
      port: parseInt(process.env.BULL_REDIS_PORT || process.env.REDIS_PORT || '6379', 10),
      password: process.env.BULL_REDIS_PASSWORD || process.env.REDIS_PASSWORD || undefined,
      db: 1,
      keyPrefix: 'bull:',
      maxRetriesPerRequest: null,
      enableReadyCheck: false,
      enableOfflineQueue: true,
    },
    defaultJobOptions: {
      attempts: 3,
      backoff: { type: 'exponential', delay: 2000 },
      removeOnComplete: { count: 100, age: 3600 },
      removeOnFail: { count: 500, age: 86400 },
    },
    queues: {
      email: { name: 'email-queue', concurrency: 3, limiter: { max: 100, duration: 60000 } },
      notification: { name: 'notification-queue', concurrency: 5, limiter: { max: 200, duration: 60000 } },
      analytics: { name: 'analytics-queue', concurrency: 2, limiter: { max: 50, duration: 60000 } },
      media: { name: 'media-queue', concurrency: 2, limiter: { max: 20, duration: 60000 } },
      post: { name: 'post-queue', concurrency: 3, limiter: { max: 100, duration: 60000 } },
    },
    bullBoard: {
      enabled: false,
      basePath: '/admin/queues',
      username: 'admin',
      password: 'password',
    },
  },
  worker: { concurrency: 5, lockDuration: 30000 },
  priorities: { critical: 1, high: 2, normal: 3, low: 4 },
};

export default queueConfig;