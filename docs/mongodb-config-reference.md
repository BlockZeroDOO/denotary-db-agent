# MongoDB Config Reference

This document describes MongoDB-specific configuration.

## Current Status

MongoDB is still a scaffold adapter.

Declared target path:

- snapshot support
- change stream capture

## `connection`

Expected keys:

- `uri`

## `options`

Planned target path:

- `capture_mode`
- collection selection and change stream behavior

## Notes

- capability metadata currently declares:
  - `capture_modes = ("snapshot", "change_streams")`
- target runtime assumes replica set or sharded deployment
