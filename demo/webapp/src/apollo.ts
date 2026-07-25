import { ApolloClient, HttpLink, InMemoryCache, split } from '@apollo/client';
import { GraphQLWsLink } from '@apollo/client/link/subscriptions';
import { getMainDefinition } from '@apollo/client/utilities';
import { createClient } from 'graphql-ws';

const HTTP_URL = import.meta.env.VITE_VS_HTTP ?? 'http://127.0.0.1:8092/graphql';
const WS_URL = import.meta.env.VITE_VS_WS ?? 'ws://127.0.0.1:8092/graphql/ws';
const TENANT = import.meta.env.VITE_VS_TENANT ?? 'acme';
const AUTH_TOKEN = import.meta.env.VITE_VS_TOKEN ?? 'demo-token';

/** Allow the UI to observe + control the underlying WS session. */
export interface WsState {
  status: 'connecting' | 'connected' | 'closed' | 'error';
  attempts: number;
  lastEventAt: number | null;
}

export const wsState: WsState = {
  status: 'connecting',
  attempts: 0,
  lastEventAt: null,
};

/** Subscribers notified whenever wsState changes. */
const wsListeners = new Set<() => void>();
export function subscribeWs(listener: () => void): () => void {
  wsListeners.add(listener);
  return () => wsListeners.delete(listener);
}
function notifyWs() {
  wsListeners.forEach((l) => l());
}

const wsClient = createClient({
  url: WS_URL,
  connectionParams: { authToken: AUTH_TOKEN, tenant: TENANT },
  shouldRetry: () => true,
  retryAttempts: Infinity,
  retryWait: async (retries) => {
    const delay = Math.min(1000 * 2 ** retries, 10_000);
    return new Promise((r) => setTimeout(r, delay));
  },
  on: {
    connecting: () => {
      wsState.status = 'connecting';
      wsState.attempts += 1;
      notifyWs();
    },
    connected: () => {
      wsState.status = 'connected';
      notifyWs();
    },
    closed: () => {
      wsState.status = 'closed';
      notifyWs();
    },
    error: () => {
      wsState.status = 'error';
      notifyWs();
    },
  },
});

const wsLink = new GraphQLWsLink(wsClient);
const httpLink = new HttpLink({ uri: HTTP_URL });

const splitLink = split(
  ({ query }) => {
    const def = getMainDefinition(query);
    return def.kind === 'OperationDefinition' && def.operation === 'subscription';
  },
  wsLink,
  httpLink,
);

export const client = new ApolloClient({
  link: splitLink,
  cache: new InMemoryCache(),
  defaultOptions: { watchQuery: { fetchPolicy: 'no-cache' } },
});

export function disposeWs() {
  wsClient.dispose();
}
