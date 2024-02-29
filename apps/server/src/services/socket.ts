import { Server } from 'socket.io';
import Redis from 'ioredis';
import prismaClient from './prisma';
import { produceMessage } from './kafka';

const pub = new Redis({
  host: 'redis-21c24700-sanjaym22-2002.a.aivencloud.com',
  port: 19957,
  username: 'default',
  password: 'AVNS_43xhJWYFtCXbNE3ptQb',
});
const sub = new Redis({
  host: 'redis-21c24700-sanjaym22-2002.a.aivencloud.com',
  port: 19957,
  username: 'default',
  password: 'AVNS_43xhJWYFtCXbNE3ptQb',
});

class SocketService {
  private _io: Server;
  constructor() {
    console.log('Init socket service');
    this._io = new Server({
      cors: {
        allowedHeaders: ['*'],
        origin: '*',
      },
    });
    sub.subscribe('MESSAGES');
  }

  public initListeners() {
    const io = this._io;
    console.log('Initialising socket listeners....');
    io.on('connect', (socket) => {
      console.log('New socket connected', socket.id);
      socket.on('event:message', async ({ message }: { message: string }) => {
        console.log('New message recieved', message);
        // publish this message to redis
        await pub.publish('MESSAGES', JSON.stringify({ message }));
      });
    });

    sub.on('message', async (channel, message) => {
      if (channel === 'MESSAGES') {
        console.log('new message from redis', message);
        io.emit('message', message);
        await produceMessage(message);
        console.log('Message Produced to Kafka Broker');
      }
    });
  }

  get io() {
    return this._io;
  }
}

export default SocketService;
