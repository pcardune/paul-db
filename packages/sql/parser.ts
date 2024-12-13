import SQLParser, {
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

/**
 * This is a replacement for the same type in the node-sql-parser package.
 *
 * It corrects the `columns` property to also be SQLParser.ValueExpr<string>[],
 * which is what you get when parsing for postgresql
 */
export interface Insert_Replace {
  type: "replace" | "insert"
  table: any
  columns: string[] | null | SQLParser.ValueExpr<string>[]
  values: SQLParser.InsertReplaceValue[] | SQLParser.Select
  partition: any[]
  prefix: string
  on_duplicate_update: {
    keyword: "on duplicate key update"
    set: SQLParser.SetList[]
  }
  loc?: SQLParser.LocationRange
}

/**
 * This is a replacement for the same type in the node-sql-parser package.
 *
 * it corrects the distinct property, which can be {type: null}
 */
export interface Select {
  with: SQLParser.With[] | null
  type: "select"
  options: any[] | null
  distinct: "DISTINCT" | null | { type: null }
  columns: any[] | SQLParser.Column[]
  from: SQLParser.From[] | SQLParser.TableExpr | null
  where: Binary | Function | null
  groupby: {
    columns: ColumnRef[] | null
    modifiers: SQLParser.ValueExpr<string>[]
  }
  having: any[] | null
  orderby: SQLParser.OrderBy[] | null
  limit: SQLParser.Limit | null
  window?: SQLParser.WindowExpr
  qualify?: any[] | null
  _orderby?: SQLParser.OrderBy[] | null
  _limit?: SQLParser.Limit | null
  parentheses_symbol?: boolean
  _parentheses?: boolean
  loc?: SQLParser.LocationRange
  _next?: Select
  set_op?: string
}

export function isInsertReplace(
  t: SQLParser.AST | Insert_Replace,
): t is Insert_Replace {
  return t.type === "insert" || t.type === "replace"
}

export function isSelect(t: SQLParser.AST | Select): t is Select {
  return t.type === "select"
}
