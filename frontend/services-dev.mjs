// Exercise the production /assets/ paths under vercel dev's injected PORT.
// Vite's source/HMR URLs would need additional public routes to this service.
import {build, preview} from 'vite';

await build();
const server = await preview({preview: {
  host: '127.0.0.1', port: Number(process.env.PORT || 5173), strictPort: true
}});
server.printUrls();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => server.httpServer.close(() => process.exit(0)));
}
