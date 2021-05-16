import amqp from 'amqplib';
import EventEmitter from 'events';
import { IntegrationEvent } from './event';
import { IRabbitMQConfig, IIntegrationEvent, ILogger, ISystemNotify } from './definitions';
import { waitByPromise } from './helpers';

const WAITING_TIME_AMPLITUDE = 1000; // ms
const MAXIMUM_CONNECTION_RETRY_COUNT = 5;
const MAXIMUM_PUBLISHING_RETRY_COUNT = 5;
const NOT_CONNECTED = 0;
const CONNECTED = 1;

interface IMsgFields {
  consumerTag: string;
  deliveryTag: number;
  redelivered: boolean;
  exchange: string;
  routingKey: string;
}

interface IMsgProperties {
  contentType: string;
  contentEncoding: string;
  headers: {
    'x-death': unknown[];
    'x-first-death-exchange': string;
    'x-first-death-queue': string;
    'x-first-death-reason': string;
  };
  deliveryMode: number;
  priority: unknown;
  correlationId: unknown;
  replyTo: unknown;
  expiration: unknown;
  messageId: unknown;
  timestamp: unknown;
  type: unknown;
  userId: unknown;
  appId: unknown;
  clusterId: unknown;
}

export interface IMsg {
  fields: IMsgFields;
  properties: IMsgProperties;
}

type SubscribeProp = {
  eventName: string;
  listener: (event: IIntegrationEvent, done: (arg?: Error) => void, msg: IMsg) => void;
  options?: {
    noAck: boolean;
    retryCount: number;
  };
};

export interface IEventBus {
  unsubscribe: (eventName: string) => Promise<void>;
  subscribe: (
    eventName: string,
    listener: (event: IIntegrationEvent, done: (arg?: Error) => void, msg: IMsg) => void,
    options?: { noAck: boolean; retryCount: number }
  ) => Promise<void>;
  publish: (integrationEvent: IIntegrationEvent) => Promise<boolean>;
  connect: () => Promise<void>;
}

export class RabbitMQ implements IEventBus {
  consumer: string;
  exchange: string;
  hostname: string;
  port: number;
  username: string;
  password: string;
  connectionStatus: number;
  connectionRetryAttempt: number;
  connection!: amqp.Connection;
  subscriptions: SubscribeProp[];
  channel!: amqp.Channel;

  logger: ILogger;
  systemNotify?: ISystemNotify;
  eventEmitter: EventEmitter;

  constructor({
    config = {
      consumer: 'Example',
      exchange: 'ecample.event_bus',
      hostname: 'localhost',
      port: 5672,
      username: 'guest',
      password: 'guest'
    },
    logger = console,
    systemNotify,
    eventEmitter
  }: {
    config?: IRabbitMQConfig;
    logger?: ILogger;
    systemNotify?: ISystemNotify;
    eventEmitter: EventEmitter;
  }) {
    this.logger = logger;
    this.systemNotify = systemNotify;
    this.eventEmitter = eventEmitter;

    this.consumer = config.consumer;
    this.exchange = config.exchange;
    this.hostname = config.hostname;
    this.port = config.port;
    this.username = config.username;
    this.password = config.password;
    this.connectionStatus = NOT_CONNECTED;
    this.connectionRetryAttempt = 0;
    this.subscriptions = [];
  }

  async unsubscribe(eventName: string): Promise<void> {
    await this.createChannel();
    await this.channel.unbindQueue(this.consumer, this.exchange, eventName);
    this.eventEmitter.removeAllListeners(eventName);
  }

  async subscribe(
    eventName: string,
    listener: (event: IIntegrationEvent, done: (arg?: Error) => void, msg: IMsg) => void,
    options?: {
      noAck: boolean;
      retryCount: number;
    }
  ): Promise<void> {
    this.logger.info(`Subscribing event ${eventName} with ${listener.name}`);

    const mergeOptions = {
      noAck: false,
      retryCount: 3,
      ...options
    };

    await this.createChannel();

    await this.channel.assertQueue(this.consumer);
    await this.channel.bindQueue(this.consumer, this.exchange, this.createRoutingKey(eventName));
    this.eventEmitter.on(eventName, listener);
    this.channel.consume(
      this.consumer,
      (msg: amqp.ConsumeMessage | null) => {
        try {
          this.logger.info(`Receiving RabbitMQ event`);
          let parseJSON;
          try {
            if (!msg) throw new Error();

            parseJSON = JSON.parse(msg.content.toString());
            if (!parseJSON.name) throw new Error();
          } catch (error) {
            this.logger.warn(`Failed to handle integration event. Invalid event format`);
            // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
            this.channel.ack(msg!);
            return;
          }

          const integrationEvent = new IntegrationEvent(parseJSON);
          this.logger.info(`Processing RabbitMQ event ${integrationEvent.name}`);
          this.eventEmitter.emit(
            integrationEvent.name,
            integrationEvent,
            (err: Error) => {
              try {
                if (err) {
                  if (+msg.fields.deliveryTag > mergeOptions.retryCount) {
                    this.channel.nack(msg, false, false);
                    this.logger.warn(
                      `Failed to handle integration event ${integrationEvent.id} `,
                      err
                    );
                  } else {
                    this.channel.nack(msg);
                    this.sendErrorToTelegram(
                      `🟧 RabbitMQ - Handle integration event ${integrationEvent.name} error`,
                      err
                    );
                    this.logger.warn(
                      `Failed to handle integration event ${integrationEvent.id} `,
                      err
                    );
                  }
                } else {
                  this.channel.ack(msg);
                  this.logger.info(`Handle successfully integration event ${integrationEvent.id}`);
                }
              } catch (error) {
                this.channel.nack(msg, false, false);
                this.sendErrorToTelegram(
                  `🟧 RabbitMQ - Handle integration event ${integrationEvent.name} error`,
                  error
                );
                this.logger.warn(
                  `Failed to handle integration event ${integrationEvent.id} `,
                  error
                );
              }
            },
            {
              fields: msg.fields,
              properties: msg.properties
            }
          );
        } catch (error) {
          this.sendErrorToTelegram(`🟧 RabbitMQ - Handle processing message ${msg} error`, error);
          this.logger.warn(`ERROR Processing message ${msg} `, error);
        }
      },
      options
    );
    this.subscriptions.push({ eventName, listener, options });
  }

  async publish(integrationEvent: IIntegrationEvent): Promise<boolean> {
    let isPublished = false;
    for (
      let publishingRetryAttempt = 0;
      publishingRetryAttempt < MAXIMUM_PUBLISHING_RETRY_COUNT;
      publishingRetryAttempt++
    ) {
      try {
        this.logger.info(
          `Creating RabbitMQ channel to publish event: ${integrationEvent.id} (${integrationEvent.name})`
        );
        await this.createChannel();

        this.logger.info(`Publishing event to RabbitMQ: ${integrationEvent.id}`);
        isPublished = this.channel.publish(
          this.exchange,
          this.createRoutingKey(integrationEvent.name),
          Buffer.from(JSON.stringify(integrationEvent)),
          {
            mandatory: true,
            persistent: true
          }
        );
      } catch (error) {
        this.sendErrorToTelegram(`🟧 RabbitMQ - Publish event ${integrationEvent.id} error`, error);
        this.logger.warn(`Could not publish event: ${integrationEvent.id} `, error);
      }

      if (isPublished) {
        this.logger.info(`Published event to RabbitMQ: ${integrationEvent.id}`);
        return isPublished;
      } else {
        const waitingTime = publishingRetryAttempt * WAITING_TIME_AMPLITUDE;
        this.logger.info(
          `Re-publishing event to RabbitMQ : ${integrationEvent.id} after ${waitingTime} ms`
        );
        await waitByPromise(waitingTime);
      }
    }

    if (!isPublished) {
      this.sendErrorToTelegram(
        `🟧 RabbitMQ - Publish event ${integrationEvent.id} after trying ${MAXIMUM_PUBLISHING_RETRY_COUNT} times error`,
        ''
      );
      this.logger.warn(
        `Could not publish event ${integrationEvent.id} after trying ${MAXIMUM_PUBLISHING_RETRY_COUNT} times`
      );
    }

    return isPublished;
  }

  private createRoutingKey(eventName: string) {
    return eventName;
  }

  async createChannel(): Promise<void> {
    if (!this.isConnected()) {
      await this.connect();
    }

    if (this.channel) {
      return;
    }

    this.channel = await this.connection.createChannel();
    this.channel.assertExchange(this.exchange, 'topic', { durable: true });
  }

  async connect(): Promise<void> {
    if (this.isConnected()) {
      return;
    }

    if (!this.isConnected() && this.isExceededConnectionRetryAttempt()) {
      // throw new Error(`RabbitMQ Client could not connect`);
      await this.sendErrorToTelegram(`🟧 RabbitMQ - Exit`, 'Maximum trying re-connect');
      process.exit(1);
    }

    try {
      this.logger.info(`RabbitMQ Client is trying to connect`);

      this.connection = await amqp.connect({
        hostname: this.hostname,
        port: this.port,
        username: this.username,
        password: this.password
      });
      this.connectionStatus = CONNECTED;
      this.connectionRetryAttempt = 0;

      await this.createChannel();
      await this.resubscribe();
      this.logger.info(`RabbitMQ Client connected`);

      this.connection.on('error', async (error) => {
        this.sendErrorToTelegram(`🟧 RabbitMQ - Connections could not connect`, error);
        this.logger.error(`RabbitMQ connections could not connect`, error);
        this.connectionStatus = NOT_CONNECTED;
        await waitByPromise(60000);
        await this.connect();
      });

      this.connection.on('close', async () => {
        this.sendErrorToTelegram(`🟧 RabbitMQ - Connections is close`, '');
        this.logger.warn('RabbitMQ connections is close');
        this.connectionStatus = NOT_CONNECTED;
        await waitByPromise(60000);
        await this.connect();
      });
    } catch (error) {
      this.logger.error(error.message);
      this.connectionStatus = NOT_CONNECTED;
    }

    if (!this.isConnected()) {
      this.connectionRetryAttempt++;
      const waitingTime = this.connectionRetryAttempt * WAITING_TIME_AMPLITUDE;

      this.logger.info(`RabbitMQ client trying re-connect after ${waitingTime} ms`);

      await waitByPromise(waitingTime);
      await this.connect();
    }
  }

  private async resubscribe() {
    if (this.subscriptions.length >= 1) {
      this.logger.info(`Resubscribe events at ${new Date().toISOString()}...`);
      try {
        await Promise.all(
          this.subscriptions.map((subscription) =>
            this.subscribe(subscription.eventName, subscription.listener, subscription.options)
          )
        );
      } catch (error) {
        this.logger.error(`Resubscribe events at ${new Date().toISOString()} failed`, error);
        await this.sendErrorToTelegram('🟧 RabbitMQ - Exit: Resubscribe events failed', error);
        process.exit(1);
      }
    }
  }

  private isExceededConnectionRetryAttempt() {
    return this.connectionRetryAttempt > MAXIMUM_CONNECTION_RETRY_COUNT;
  }

  private isConnected() {
    return this.connectionStatus === CONNECTED;
  }

  private async sendErrorToTelegram(title: string, error?: any): Promise<void> {
    if (!this.systemNotify) return;
    return this.systemNotify.sendErrorToTelegram(title, error);
  }
}
