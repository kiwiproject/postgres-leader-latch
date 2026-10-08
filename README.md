### Postgres Leader Latch

[![Build](https://github.com/kiwiproject/postgres-leader-latch/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/kiwiproject/postgres-leader-latch/actions/workflows/build.yml?query=branch%3Amain)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=kiwiproject_postgres-leader-latch&metric=alert_status)](https://sonarcloud.io/dashboard?id=kiwiproject_postgres-leader-latch)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=kiwiproject_postgres-leader-latch&metric=coverage)](https://sonarcloud.io/dashboard?id=kiwiproject_postgres-leader-latch)
[![CodeQL](https://github.com/kiwiproject/postgres-leader-latch/actions/workflows/codeql.yml/badge.svg)](https://github.com/kiwiproject/postgres-leader-latch/actions/workflows/codeql.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A framework-independent Java library for leader election (a leader latch) among multiple instances of the
same logical service, using Postgres advisory locks as the backend.

It is the Postgres counterpart to [dynamodb-leader-latch](https://github.com/kiwiproject/dynamodb-leader-latch)
and [dropwizard-leader-latch](https://github.com/kiwiproject/dropwizard-leader-latch), and has no Curator,
ZooKeeper, Dropwizard, or Helidon dependency.

> Status: under development. Not yet released.
