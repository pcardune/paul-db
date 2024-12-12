import {
  AggrFunc,
  Binary,
  Case,
  Cast,
  ColumnRef,
  ColumnRefItem,
  ExpressionValue,
  ExprList,
  Function,
  Interval,
  Param,
  Value,
} from "npm:node-sql-parser"

export function isColumnRefItem(
  t: ColumnRef | ExpressionValue,
): t is ColumnRefItem {
  return t.type === "column_ref"
}
export function isColumnRef(t: ExpressionValue): t is ColumnRef {
  return t.type === "column_ref" || t.type === "expr"
}
export function isParam(t: ExpressionValue): t is Param {
  return t.type === "param"
}
export function isFunction(t: ExpressionValue): t is Function {
  return t.type === "function"
}
export function isCase(t: ExpressionValue): t is Case {
  return t.type === "case"
}
export function isAggrFunc(t: ExpressionValue): t is AggrFunc {
  return t.type === "aggr_func"
}
export function isBinary(t: ExpressionValue): t is Binary {
  return t.type === "binary_expr"
}
export function isCast(t: ExpressionValue): t is Cast {
  return t.type === "cast"
}
export function isInterval(t: ExpressionValue): t is Interval {
  return t.type === "interval"
}
export function isValue(t: ExpressionValue): t is Value {
  return !isColumnRef(t) && !isParam(t) && !isFunction(t) && !isCase(t) &&
    !isAggrFunc(t) && !isBinary(t) && !isCast(t) && !isInterval(t)
}

export function isExprList(expr: ExpressionValue | ExprList): expr is ExprList {
  return expr.type === "expr_list"
}
export function isExpressionValue(
  expr: ExpressionValue | ExprList,
): expr is ExpressionValue {
  return !isExprList(expr)
}
