// src/config/neo4j.ts

import neo4j, { Driver, Session, Result, ManagedTransaction } from 'neo4j-driver';
import environmentConfig from '../env/env';
import logger, { LogCategory, LogCategoryType } from '@/shared/logger.util';
import { ErrorResponse, HttpStatus } from '@/shared/response.util';

interface Neo4jConfig {
  uri: string;
  username: string;
  password: string;
  database: string;
  connectTimeout: number;
  maxConnectionLifetime: number;
  connectionAcquisitionTimeout: number;
  maxConnectionPoolSize: number;
  loggingLevel: string;
  maxRetries: number;
  retryDelayMs: number;
  healthCheckIntervalMs: number;
}

interface LogMetadata {
  category: LogCategoryType;
  [key: string]: any;
}

interface ConnectionMetrics {
  totalQueries: number;
  activeConnections: number;
  failedConnections: number;
  avgResponseTime: number;
  lastConnectionTime: Date | null;
  uptime: number;
}

interface HealthStatus {
  connected: boolean;
  database: string;
  uri: string;
  latencyMs?: number;
  error?: string;
  timestamp: Date;
}

enum ConnectionState {
  DISCONNECTED = 'disconnected',
  CONNECTING = 'connecting',
  CONNECTED = 'connected',
  FAILED = 'failed'
}

class Neo4jConnection {
  private static instance: Neo4jConnection;
  private driver: Driver | null = null;
  private config: Neo4jConfig;
  private connectionState: ConnectionState = ConnectionState.DISCONNECTED;
  private connectionPromise: Promise<void> | null = null;
  private retryCount: number = 0;
  private healthCheckInterval: NodeJS.Timeout | null = null;
  private startTime: Date = new Date();

  private metrics: ConnectionMetrics = {
    totalQueries: 0,
    activeConnections: 0,
    failedConnections: 0,
    avgResponseTime: 0,
    lastConnectionTime: null,
    uptime: 0
  };

  private constructor() {
    this.config = {
      uri: (environmentConfig as any).NEO4J_URI || 'bolt://localhost:7687',
      username: (environmentConfig as any).NEO4J_USERNAME || 'neo4j',
      password: (environmentConfig as any).NEO4J_PASSWORD || 'password',
      database: (environmentConfig as any).NEO4J_DATABASE || 'neo4j',
      connectTimeout: (environmentConfig as any).NEO4J_CONNECT_TIMEOUT || 15000,
      maxConnectionLifetime: (environmentConfig as any).NEO4J_MAX_CONNECTION_LIFETIME || 1800000,
      connectionAcquisitionTimeout: (environmentConfig as any).NEO4J_CONNECTION_ACQUISITION_TIMEOUT || 30000,
      maxConnectionPoolSize: (environmentConfig as any).NEO4J_MAX_CONNECTION_POOL_SIZE || 50,
      loggingLevel: (environmentConfig as any).NEO4J_LOGGING_LEVEL || 'warn',
      maxRetries: (environmentConfig as any).NEO4J_MAX_RETRIES || 3,
      retryDelayMs: (environmentConfig as any).NEO4J_RETRY_DELAY_MS || 2000,
      healthCheckIntervalMs: (environmentConfig as any).NEO4J_HEALTH_CHECK_INTERVAL_MS || 60000
    };

    logger.info('Neo4jConnection initialized', this.safeNormalizeMetadata({
      uri: this.config.uri,
      database: this.config.database,
      maxPoolSize: this.config.maxConnectionPoolSize,
      category: LogCategory.DATABASE,
    }));

    this.initializeConnection();
    this.startHealthChecks();
  }

  private safeNormalizeMetadata(data: any): LogMetadata {
    if (!data || typeof data !== 'object') {
      return { category: LogCategory.DATABASE };
    }

    try {
      const normalized = JSON.parse(JSON.stringify(data, (_key, value) => {
        if (value instanceof Error) {
          return {
            name: value.name,
            message: value.message,
            stack: value.stack?.split('\n').slice(0, 3).join('\n')
          };
        }

        if (typeof value === 'function' || value === undefined) {
          return '[Function]';
        }

        if (typeof value === 'object' && value !== null) {
          const keys = Object.keys(value);
          if (keys.length > 10) {
            const limited: any = {};
            keys.slice(0, 10).forEach(k => limited[k] = value[k]);
            limited._truncated = `${keys.length - 10} more keys`;
            return limited;
          }
        }

        return value;
      }));

      if (!normalized.category) {
        normalized.category = LogCategory.DATABASE;
      }

      return normalized;
    } catch (error: any) {
      return {
        category: LogCategory.DATABASE,
        error: 'Unable to serialize metadata',
        originalType: typeof data
      };
    }
  }

  public static getInstance(): Neo4jConnection {
    if (!Neo4jConnection.instance) {
      Neo4jConnection.instance = new Neo4jConnection();
    }
    return Neo4jConnection.instance;
  }

  private initializeConnection(): void {
    void this.connectWithRetry().catch((error) => {
      logger.warn('Neo4j unavailable - degraded mode', {
        error: error?.message || String(error),
      });
      this.connectionState = ConnectionState.FAILED;
    });
    this.connectionPromise = Promise.resolve();
  }

  private async connectWithRetry(): Promise<void> {
    while (this.retryCount < this.config.maxRetries) {
      try {
        await this.connect();
        this.retryCount = 0;
        return;
      } catch (error: any) {
        this.retryCount++;
        this.metrics.failedConnections++;

        if (this.retryCount >= this.config.maxRetries) {
          this.connectionState = ConnectionState.FAILED;
          return;
        }

        const delay = this.config.retryDelayMs * this.retryCount;
        logger.warn(`Neo4j retry ${this.retryCount}/${this.config.maxRetries} in ${delay}ms`, this.safeNormalizeMetadata({
          category: LogCategory.DATABASE,
          retryCount: this.retryCount,
          delay
        }));

        await this.delay(delay);
      }
    }
  }

  private async connect(): Promise<void> {
    try {
      this.connectionState = ConnectionState.CONNECTING;

      this.driver = neo4j.driver(
        this.config.uri,
        neo4j.auth.basic(this.config.username, this.config.password),
        {
          encrypted: false,
          trust: 'TRUST_ALL_CERTIFICATES',
          maxConnectionLifetime: this.config.maxConnectionLifetime,
          maxConnectionPoolSize: this.config.maxConnectionPoolSize,
          connectionAcquisitionTimeout: this.config.connectionAcquisitionTimeout,
          connectionTimeout: this.config.connectTimeout,
          disableLosslessIntegers: true,
          logging: {
            level: this.config.loggingLevel as 'error' | 'warn' | 'info' | 'debug',
            logger: (level: string, message: string) => {
              if (level === 'error') {
                logger.error(`[Neo4j] ${message}`, this.safeNormalizeMetadata({
                  category: LogCategory.DATABASE,
                  level
                }));
              }
            },
          },
        }
      );

      await Promise.race([
        this.driver.verifyConnectivity(),
        this.createTimeout(this.config.connectTimeout, 'Connection timeout')
      ]);

      this.connectionState = ConnectionState.CONNECTED;
      this.metrics.lastConnectionTime = new Date();

      logger.info('Neo4j connected', this.safeNormalizeMetadata({
        uri: this.config.uri,
        poolSize: this.config.maxConnectionPoolSize,
        category: LogCategory.DATABASE,
      }));

    } catch (error: unknown) {
      this.connectionState = ConnectionState.FAILED;
      const errorMessage = error instanceof Error ? error.message : String(error);

      logger.error('Neo4j connection failed', this.safeNormalizeMetadata({
        error: errorMessage,
        category: LogCategory.DATABASE
      }));

      if (this.driver) {
        await this.driver.close();
        this.driver = null;
      }

      throw new ErrorResponse(
        `Neo4j connection failed: ${errorMessage}`,
        HttpStatus.SERVICE_UNAVAILABLE,
        'NEO4J_001'
      );
    }
  }

  private startHealthChecks(): void {
    this.healthCheckInterval = setInterval(async () => {
      if (this.connectionState === ConnectionState.CONNECTED) {
        try {
          await this.quickHealthCheck();
        } catch (error: any) {
          logger.warn('Health check failed, will retry connection', this.safeNormalizeMetadata({
            category: LogCategory.DATABASE,
            error: error instanceof Error ? error.message : String(error)
          }));
          this.connectionState = ConnectionState.FAILED;
          this.connectionPromise = this.connectWithRetry();
        }
      }
    }, this.config.healthCheckIntervalMs);
  }

  private async quickHealthCheck(): Promise<void> {
    if (!this.driver) return;

    const session = this.driver.session({ database: this.config.database });
    try {
      await session.run('RETURN 1');
    } finally {
      await session.close();
    }
  }

  public async getDriver(): Promise<Driver> {
    await this.ensureConnected();

    if (!this.driver || this.connectionState !== ConnectionState.CONNECTED) {
      throw new ErrorResponse(
        'Neo4j not available',
        HttpStatus.SERVICE_UNAVAILABLE,
        'NEO4J_UNAVAILABLE'
      );
    }

    return this.driver;
  }

  public async createSession(database?: string): Promise<Session> {
    const driver = await this.getDriver();

    const session = driver.session({
      database: database || this.config.database,
      defaultAccessMode: neo4j.session.WRITE,
    });

    this.metrics.activeConnections++;

    const originalClose = session.close.bind(session);
    session.close = async () => {
      this.metrics.activeConnections = Math.max(0, this.metrics.activeConnections - 1);
      await originalClose();
    };

    return session;
  }

  public async runQuery(
    cypher: string,
    parameters?: Record<string, any>,
    database?: string
  ): Promise<Result> {
    const startTime = Date.now();
    const session = await this.createSession(database);

    try {
      this.metrics.totalQueries++;
      const result = await session.run(cypher, parameters);

      const responseTime = Date.now() - startTime;
      this.metrics.avgResponseTime = Math.round(
        (this.metrics.avgResponseTime * 0.9) + (responseTime * 0.1)
      );

      return result;
    } catch (error: any) {
      this.metrics.failedConnections++;
      throw error;
    } finally {
      await session.close();
    }
  }

  public async runTransaction<T>(
    work: (tx: ManagedTransaction) => Promise<T>,
    database?: string
  ): Promise<T> {
    const session = await this.createSession(database);

    try {
      return await session.executeWrite(work);
    } finally {
      await session.close();
    }
  }

  private async ensureConnected(): Promise<void> {
    if (this.connectionState === ConnectionState.CONNECTED && this.driver) {
      return;
    }

    if (!this.connectionPromise || this.connectionState === ConnectionState.FAILED) {
      this.connectionPromise = this.connectWithRetry();
    }

    await this.connectionPromise;
  }

  public async close(): Promise<void> {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
    }

    if (this.driver) {
      await this.driver.close();
      this.driver = null;
      this.connectionState = ConnectionState.DISCONNECTED;

      logger.info('Neo4j connection closed', this.safeNormalizeMetadata({
        category: LogCategory.DATABASE
      }));
    }
  }

  public async verifyConnectivity(): Promise<boolean> {
    try {
      if (!this.driver || this.connectionState !== ConnectionState.CONNECTED) {
        return false;
      }

      await this.driver.verifyConnectivity();
      return true;
    } catch (error: any) {
      this.connectionState = ConnectionState.FAILED;
      logger.error('Neo4j connectivity verification failed', this.safeNormalizeMetadata({
        error: error instanceof Error ? error.message : String(error),
        category: LogCategory.DATABASE
      }));
      return false;
    }
  }

  public async checkHealth(): Promise<HealthStatus> {
    const startTime = Date.now();

    try {
      const connected = await this.verifyConnectivity();

      return {
        connected,
        database: this.config.database,
        uri: this.config.uri,
        latencyMs: Date.now() - startTime,
        timestamp: new Date()
      };
    } catch (error: unknown) {
      return {
        connected: false,
        database: this.config.database,
        uri: this.config.uri,
        error: error instanceof Error ? error.message : String(error),
        timestamp: new Date()
      };
    }
  }

  public getMetrics(): ConnectionMetrics {
    this.metrics.uptime = Date.now() - this.startTime.getTime();
    return { ...this.metrics };
  }

  public isConnected(): boolean {
    return this.connectionState === ConnectionState.CONNECTED;
  }

  public getConnectionState(): ConnectionState {
    return this.connectionState;
  }

  private createTimeout(ms: number, message: string): Promise<never> {
    return new Promise((_, reject) => {
      setTimeout(() => reject(new Error(message)), ms);
    });
  }

  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

const neo4jConnection = Neo4jConnection.getInstance();

export const getNeo4jInstance = (): Neo4jConnection => neo4jConnection;

export const createNeo4jSession = async (database?: string): Promise<Session> => {
  return await neo4jConnection.createSession(database);
};

export const runNeo4jQuery = async (
  cypher: string,
  parameters?: Record<string, any>,
  database?: string
): Promise<Result> => {
  return await neo4jConnection.runQuery(cypher, parameters, database);
};

export const runNeo4jTransaction = async <T>(
  work: (tx: ManagedTransaction) => Promise<T>,
  database?: string
): Promise<T> => {
  return await neo4jConnection.runTransaction(work, database);
};

export const getNeo4jDriver = async (): Promise<Driver> => {
    return await neo4jConnection.getDriver();
};

export const closeNeo4jConnection = async (): Promise<void> => {
  await neo4jConnection.close();
};

export const verifyNeo4jConnectivity = async (): Promise<boolean> => {
  return await neo4jConnection.verifyConnectivity();
};

export const checkNeo4jHealth = async (): Promise<HealthStatus> => {
  return await neo4jConnection.checkHealth();
};

export const checkNeo4jConnection = async (): Promise<boolean> => {
  try {
    await neo4jConnection.checkHealth();
    return true;
  } catch (error: any) {
    return false;
  }
};

export const isNeo4jConnected = (): boolean => {
  return neo4jConnection.isConnected();
};

export const getNeo4jMetrics = (): ConnectionMetrics => {
  return neo4jConnection.getMetrics();
};

export { ConnectionState, ConnectionMetrics, HealthStatus };

export default neo4jConnection;