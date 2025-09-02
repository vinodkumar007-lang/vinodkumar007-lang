📄 Git Branching Strategy & Release Management
1. Purpose

This document outlines the branching strategy to be followed for managing code across environments: Dev, ETE, QA, UAT, and Prod.
The strategy ensures:

Stable deployments to each environment

Clear traceability using tagID

Quick handling of production issues without disrupting development

Consistency of code across all environments

2. Branching Model
Permanent Branches

dev – Base branch for active development.

prod – Always reflects production code.

Release Branches (per environment)

release/ete – End-to-End environment.

release/qa – QA environment.

release/uat – UAT environment.

release/prod – Pre-production mirror for release control.

Temporary Branches

Feature branches (from dev):

feature/<tagID>-short-description


Hotfix branches (from prod):

hotfix/<tagID>-short-description

3. Normal Development Flow

Feature Creation

Developer creates branch from dev:

git checkout dev
git checkout -b feature/TAG123-new-login-api


Merge into Dev

After review, merge feature/TAG123-new-login-api → dev.

Promote Across Environments

dev → release/ete → release/qa → release/uat → release/prod → prod

Each promotion via Pull Request (PR).

Each step tagged for traceability:

TAG123-ete

TAG123-qa

TAG123-uat

TAG123-prod

4. Production Hotfix Flow

Branch from Prod

If an issue is found in Prod:

git checkout prod
git checkout -b hotfix/TAG201-fix-timeout


Apply Fix

Commit and test changes locally/QA.

Deploy Fix

Merge back into prod.

Tag as TAG201-prod.

Back-Merge into Lower Environments

Merge hotfix sequentially:

prod → release/prod → release/uat → release/qa → release/ete → dev


Tag accordingly (TAG201-uat, TAG201-qa, etc.).

5. Example Flows
Feature Flow
feature/TAG123 → dev → release/ete → release/qa → release/uat → release/prod → prod

Hotfix Flow
hotfix/TAG201 → prod → release/prod → release/uat → release/qa → release/ete → dev

6. Tagging & Traceability

Every promotion is tagged with tagID-environment.

Example:

TAG123-ete → running in ETE.

TAG123-qa → running in QA.

TAG123-prod → deployed to Prod.

7. Benefits

✅ Stability per environment – Each environment has its own controlled release branch.
✅ Traceability – tagID shows exactly where a change is deployed.
✅ Fast Production Fixes – Hotfixes go directly from prod and sync back down.
✅ No Code Loss – Fixes are always merged back into all environments.
✅ Clear Audit Trail – Every change is tracked with a tag and PR.

📌 Recommendation for Client:

Enforce Pull Request approvals for all merges.

Protect all environment branches (dev, release/*, prod) from direct commits.

Automate CI/CD pipelines to deploy code automatically when a release branch is updated.
