# Aegis Vercel services

Import this repository as one multi-service project. Keep the project root at `.`. Do not use either of the standalone "Import single project" buttons.

| Name | Root | Purpose | Public route |
| --- | --- | --- | --- |
| `app` | `.` | Python WSGI login, authenticated dashboard HTML, JSON API | `/api/*`, `/login`, `/logout`, and the final `/(.*)` catch-all including `/` |
| `frontend` | `frontend` | Independently built Vite HTML, JavaScript and CSS | `/assets/*` only; its root HTML is reached internally |
| `aegis-worker` | `.` | Continuous market/trading loop, defined under `daemons` | None; no incoming HTTP traffic |

## Runtime call graph

`app` declares a binding to `frontend`: `type=service`, `service=frontend`, `format=url`, `env=AEGIS_FRONTEND_URL`. Vercel generates the URL at runtime. Do not set the variable in Vercel settings, `.env` files, browser code, or a build command.

After checking the owner's session, `app` retrieves the frontend's built HTML through that binding and inserts the cloud marker and session CSRF token. It sends no login cookie or account credentials to the frontend. The browser then requests JavaScript/CSS under `/assets/` and JSON under `/api/` on the same public domain. The original `/api/` prefix is preserved; no prefix stripping or localhost URL is required. The frontend has no server-side calls to `app`, so it has no reverse binding. Static Vite code cannot read runtime service bindings.

The worker and API exchange durable commands, credentials and snapshots through the same PostgreSQL database. Neither calls the other over HTTP; neither needs an HTTP service binding for that connection. `aegis-worker` remains a daemon rather than a request-driven Python service. It has no public rewrite.

## Builds and local development

Each service builds independently. Only `frontend` runs `npm ci` and `npm run build`, producing `dist`. The Python service uses `api.index:app`; its `pyproject.toml` packages only `aegis*` and `api*` and reads dependencies from `requirements.txt`, preventing flat-layout discovery from treating the frontend as a Python package. It does not depend on another service's build files being present in its function bundle. The old `frontend/dist/index.html` function inclusion is removed. For a direct local Python preview outside Vercel, the existing local frontend build fallback remains available.

With a current Vercel CLI and Python/Node dependencies installed, run `vercel dev --local` (formerly `-L`) from the repository root to run Services locally without Vercel login. Vercel injects the binding. The frontend's `dev:services` command builds and previews the same `/assets/` output on Vercel's injected `PORT`, so source/HMR routes do not need to be exposed. Use the same test-only database/login configuration for both services. This command is for the web services; it does not establish production daemon operation. Production daemon operation still requires deployment, persistent database access and supported Binance connectivity.

Test the login page, authenticated `/`, `/api/state`, `/assets/` scripts/CSS, and logout. The frontend template must never be served directly at public `/index.html` without going through the login service. A missing binding or inaccessible frontend returns an unavailable response instead of a partially initialized dashboard.

## Confirmation before publishing this change

Confirm these names and routes: `app` handles the authenticated site and API, `frontend` exposes only `/assets/*` with its HTML internal, and `aegis-worker` has no public route. Confirm the only HTTP binding is `app` → `frontend` through `AEGIS_FRONTEND_URL`.

This routing change does not arm real trading or repair Binance API permissions. The existing explicit START REAL flow remains required.

Official references: [Services](https://vercel.com/docs/services), [routing](https://vercel.com/docs/services/routing), [bindings](https://vercel.com/docs/services/bindings), [configuration](https://vercel.com/docs/services/config-reference), [daemons](https://vercel.com/docs/daemons).
