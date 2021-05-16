export interface IIntegrationEvent {
  id?: string;
  name: string;
  data: Record<string, unknown>;
  createdDate?: Date;
}

export interface ILogger {
  info(message?: string, details?: any): void;
  error(message?: string, details?: any): void;
  warn(message?: string, details?: any): void;
  debug(message?: string, details?: any): void;
}

export interface IRabbitMQConfig {
  consumer: string;
  exchange: string;
  hostname: string;
  port: number;
  username: string;
  password: string;
}

export interface ISystemNotify {
  sendErrorToTelegram(title: string, error?: any): Promise<void>;
  sendSuccessMessageToTelegram(title: string): Promise<void>;
}
