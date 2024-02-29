import http from 'http';
import SocketService from './services/socket';
import { startMessageConsumer } from './services/kafka';

async function init() {
  startMessageConsumer();
  const socketService = new SocketService();
  const httpServer = http.createServer();
  const PORT = process.env.PORT ? process.env.PORT : 8000;
  socketService.io.attach(httpServer);
  httpServer.listen(PORT, () => {
    console.log(`HTTP server started at port ${PORT}`);
  });
  socketService.initListeners();
}

init();

console.log('hello from index ts server');
