# Aegis Cloud

Python trading worker, React dashboard, PostgreSQL persistence, and a single-owner login. Binance.com Spot is the real exchange adapter. GPT-6 Sol assesses fixed trend/pullback and breakout/retest candidates; deterministic code sizes orders and manages exits.

## Deploy from your private GitHub repository

1. Run **Prepare-Cloud.cmd** once on Windows. It generates `.env.production.local` and `cloud-login.txt`. Keep both files private; never upload them to GitHub. The username is `aegis`; the generated password is in `cloud-login.txt`.
2. Upload this source to a **private** GitHub repository. Import it as **one multi-service project**, with the Vercel project's root directory at the repository root. `vercel.json` declares Python `app`, Vite `frontend`, and the background `aegis-worker` daemon. Do not import the detected apps as separate projects. See [service routing and bindings](VERCEL-SERVICES.md).
3. Import the repository into Vercel. Connect a durable PostgreSQL database through Vercel Marketplace, such as Neon. Set `DATABASE_URL` to its direct/session-capable connection string with TLS. A transaction-pooling URL cannot hold the worker's session advisory lock; use a direct connection or session-mode pooler.
4. Import `.env.production.local` into the Vercel **Production** environment. The daemon and HTTP function need the same database URL and encryption key. Deploy to production; preview deployments do not run daemons.
5. Open the deployment URL and sign in using `cloud-login.txt`. Vercel injects `AEGIS_FRONTEND_URL` into `app` from its frontend binding; do not define this variable yourself or add it to a build command. The worker starts automatically in **Virtual USDT** mode. Its heartbeat, market feed and completed candles should become current after startup. Missing configuration produces an explicit setup error rather than a fabricated balance.
6. Run **Connect-Cloud.cmd** locally. Enter your own deployment's HTTPS URL and its Aegis login. Complete the official ChatGPT sign-in in the browser. The helper sends the new cloud-host registration and the already saved Binance credentials directly to that named deployment over HTTPS; it does not print or save tokens. It does not place any orders. If no saved Binance connection exists, connect the API key through the dashboard's Connections panel instead.
7. In Binance's API settings, allow the actual **outbound addresses used by both the Vercel API function and daemon**. Your PC's public IP and the site's public DNS IP are not the cloud worker's outbound IP. Verify the hosting plan's daemon networking supports the stable addresses you allowlist. Vercel's published Static IPs documentation covers Functions and builds; daemon coverage must be confirmed for your deployment. If stable daemon egress is unavailable, run `worker.py` on a persistent host with a fixed outbound IP and the same PostgreSQL database, and set the Vercel daemon instance count to zero. Keep only one active worker.
8. Verify Binance balances, account synchronization, selected account model, and order logs. Choose **Binance real wallet**, type **START REAL**, and press **Start real trading** yourself. This is the action that authorizes future exchange orders. `LIVE_TRADING_ENABLED=true` permits this control but does not itself authorize trading. Fresh prices, a valid setup, risk checks and GPT approval are still required; no trade is forced on deployment.

After the first explicit real start, the worker persists your requested mode and resumes it following successful reconnection and reconciliation. Manual **Pause entries** and **Stop new entries** persist across crashes, deployments and connection recovery. They stop new exposure; existing managed positions retain exit management and native exchange protection. To resume real entries, use Start real trading again.

## Dashboard

- Real or virtual mode, actual real balances, managed position and protection status.
- Buy/sell history includes the GPT virtual wallet, the separate no-GPT comparison wallet, and Binance-confirmed fills. Each row names the coin, wallet, time, price, quantity and commission. Select a coin to focus its chart.
- Chart-wallet selection prevents comparison trades from being mistaken for real trades or GPT-approved trades.
- Every GPT result and failed/blocked assessment is visible. A WAIT is not a fill.
- Real order intents show ACCEPTED, REJECTED, UNKNOWN or SENDING. Unknown requests are reconciled using their stable client order IDs; they are never blindly resubmitted.
- Pause entries before adding virtual USDT. Deposits are recorded separately from trading profit and cannot increase a real balance.
- Worker heartbeat, connection state, recovery reason, strategy checks and durable control results remain visible.

## Execution and operating limits

Both modes use the same completed-candle candidate engine and the same coded risk framework. Virtual execution models fills; Binance execution uses actual order/trade responses. Actual liquidity, commission assets, rounding, latency and partial fills can differ. The two modes cannot produce identical results.

One managed position, no borrowing/leverage, 80% allocation cap, $0.10 planned risk per trade, $0.50 daily loss limit, five entries/day, three consecutive losses, 10% observed drawdown limit, 15-minute cooldown, and a 30-minute position time exit. Real account commissions and Binance filters are checked. Dust remains an asset with cost basis; new entries block if residual exposure exceeds the configured cap.

The broker commits an order intent before sending. After a buy fill is reconciled, it attaches native Binance OCO stop/target orders. **There is an entry-to-OCO gap.** An outage during that interval can leave exposure without confirmed protection until reconciliation recovers. A stop is not a guaranteed fill price. Native OCO orders that Binance has confirmed remain at the exchange during model/worker outages; local time exits wait for worker connectivity.

The worker holds a PostgreSQL session advisory lock to prevent two deployments from trading concurrently. Database failure terminates the worker. A stalled-loop watchdog requests a process restart after 120 seconds. Vercel maintains the daemon instance and restarts crashed processes. No design guarantees uninterrupted connectivity or profits.

The two hypotheses remain unvalidated. The saved historical study was negative, and live GPT improvement is unmeasured. This implementation does not turn a negative study into evidence of profitability. The dashboard reports actual outcomes and can continue to WAIT indefinitely.

## Model access

Uses the exact GPT-6 Sol slug returned by the connected account's catalog. No invented fallback model and no OpenAI API key. The official subscription flow requires a local loopback sign-in helper once per cloud host. Session refresh occurs on the worker; revoked permission or exhausted allowance means WAIT. Two requests/hour, twelve/day, one in flight, per-symbol cooldown and token ceilings remain enforced. The virtual USDT balance does not cover model or hosting charges.

## Existing local records

The running Music installation and its SQLite/DPAPI files are not uploaded automatically. Its historical research and ledgers remain intact. `Connect-Cloud.cmd` imports only the Binance connection after a fresh ChatGPT sign-in. The cloud deployment starts a new durable trading ledger; pre-existing exchange holdings are protected from being sold as bot-owned inventory. Do not run another trading bot against the same account while arming this worker.

## Verification

Run `python verify.py` after installing `requirements.txt`, and `npm --prefix frontend ci` then `npm --prefix frontend run build`. The verification runner blocks external sockets and production database access. This repository's tests use deterministic exchange/model fixtures and never spend real funds. Managed PostgreSQL TLS/lock takeover, Vercel deployment, account permission, IP restrictions, and actual exchange execution need verification on the user's configured infrastructure. Local checks do not establish profitability or real-exchange execution acceptance.

## References

- [Vercel Daemons](https://vercel.com/docs/daemons) and [limits](https://vercel.com/docs/daemons/limits)
- [Vercel PostgreSQL integrations](https://vercel.com/docs/postgres)
- [Vercel outbound IP allowlisting](https://vercel.com/kb/guide/how-to-allowlist-deployment-ip-address)
- [Official ChatGPT subscription sign-in](https://developers.openai.com/siwc/token-sharing-open-source/sign-in)
- [Binance Spot trade endpoints](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/trade)

## Current account check

The existing locally saved Binance key returned HTTP 401 / Binance -2015 during a read-only check on 25 October 2026 (Asia/Karachi). This means API key, IP or permissions were rejected; it does not identify which one. Correct those settings before expecting real balances or trading. No orders were placed during the check. See the official [error definition](https://developers.binance.com/en/docs/products/spot/errors#-2015-rejected_mbx_key).
