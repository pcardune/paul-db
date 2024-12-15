/**
 * Error classes for SQL parsing and execution
 * @module
 */

/**
 * Thrown when a feature is not implemented
 */
export class NotImplementedError extends Error {}

/**
 * Thrown when a table is not found
 */
export class TableNotFoundError extends Error {}

/**
 * Thrown when a column is not found
 */
export class ColumnNotFoundError extends Error {}

/**
 * Thrown when a column is ambiguous
 */
export class AmbiguousError extends Error {}

/**
 * Thrown when a SQL parse error occurs
 */
export class SQLParseError extends Error {}
