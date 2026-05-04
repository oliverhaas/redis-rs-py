# PyPI Trusted Publisher setup (one-time)

This is the one-time setup that makes `publish.yml` able to upload to PyPI **without any long-lived API tokens stored in GitHub Secrets**. It uses GitHub's OIDC tokens via PyPI's Trusted Publisher feature.

You only need to do this once per project (and once per environment, if you also want TestPyPI).

## 1. Add the pending publisher on PyPI

The project doesn't exist on PyPI yet, so we use the "pending publisher" flow:

1. Sign in to <https://pypi.org/manage/account/publishing/>.
2. Scroll to **Add a new pending publisher**.
3. Fill in:
   - **PyPI Project Name**: `redis-rs-py`
   - **Owner**: `oliverhaas`
   - **Repository name**: `redis-rs-py`
   - **Workflow name**: `publish.yml`
   - **Environment name**: `pypi`
4. Click **Add**.

PyPI will retain the entry as "pending" until the first publish; on that first publish the project is auto-created and the entry becomes a normal Trusted Publisher.

## 2. Repeat for TestPyPI (recommended; lets you dry-run)

1. Sign in to <https://test.pypi.org/manage/account/publishing/>.
2. Same form as above, but set **Environment name** to `testpypi`.

## 3. Configure the GitHub `pypi` environment

1. Go to <https://github.com/oliverhaas/redis-rs-py/settings/environments>.
2. Click **New environment**.
3. Name: `pypi`.
4. Under **Deployment protection rules**:
   - Tick **Required reviewers** and add yourself (the maintainer).
   - Optionally tick **Wait timer** with a small value (5 min) to give yourself a window to cancel an accidental tag push.
5. Save.

## 4. Repeat for the `testpypi` environment

Same steps, environment name `testpypi`. You probably don't need required reviewers for TestPyPI — it's the dry-run target.

## 5. Verification

Once both sides are configured:

1. Trigger `publish.yml` manually with `workflow_dispatch`, target `testpypi`, version `0.1.0a2` (or whatever the next prerelease is).
2. Watch the run in <https://github.com/oliverhaas/redis-rs-py/actions/workflows/publish.yml>.
3. The `publish` job should:
   - Pause on the `testpypi` environment gate (if you set required reviewers there).
   - On approve, upload all artifacts to <https://test.pypi.org/project/redis-rs-py/>.
4. Confirm by `pip install --index-url https://test.pypi.org/simple/ redis-rs-py==0.1.0a2` in a clean venv.

If TestPyPI works, the production PyPI flow is identical — just push a `vX.Y.Z` tag and the workflow takes over.

## Troubleshooting

- **"invalid-publisher" error in the publish job log.** The values on PyPI's pending-publisher form must match the workflow exactly. Double-check the workflow filename (`publish.yml`, not `publish.yaml`) and the environment name.
- **The job never starts.** Check the `pyproject.toml` `name` field — it must equal the **PyPI Project Name** in the pending publisher (case-sensitive, hyphenation matters).
- **The job runs but gets "403 invalid token".** The repository owner / repo name on PyPI's side must match the GitHub coordinates exactly. If the repo was renamed or transferred, edit the entry on PyPI.
