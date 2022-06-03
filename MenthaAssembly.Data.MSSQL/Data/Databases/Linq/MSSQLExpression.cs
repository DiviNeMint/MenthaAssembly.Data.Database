using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;

namespace MenthaAssembly.Data.Primitives
{
    internal static class MSSQLExpression
    {
        public static string Parse<T, U>(Expression<Func<T, U>> Selector)
        {
            if (Selector.Body is MemberExpression Member &&
                Member.Expression.Equals(Selector.Parameters[0]))
                return Member.Member.Name;

            if (Selector.Body is UnaryExpression UE &&
                UE.Operand is MemberExpression ME &&
                ME.Expression.Equals(Selector.Parameters[0]))
                return ME.Member.Name;

            throw new NotSupportedException();
        }
        public static string Parse<T>(Expression<Func<T, bool>> Predict, ref List<SqlParameter> Parameters)
            => ParseExpression(Predict.Parameters[0], Predict.Body, ref Parameters);

        private static string ParseExpression(ParameterExpression Parameter, Expression Expression, ref List<SqlParameter> Values)
        {
            // Constant
            if (Expression is ConstantExpression CE)
            {
                string ArgName = $"@Arg{Values.Count}";
                Values.Add(new SqlParameter(ArgName, CE.Value));
                return ArgName;
            }

            // Property & Field
            if (Expression is MemberExpression ME)
            {
                if (Parameter.Equals(ME.Expression))
                    return ME.Member.Name;

                string ArgName = $"@Arg{Values.Count}";
                object MemberValue = ME.GetValue();
                if (MemberValue is IEnumerable)
                    throw new NotSupportedException();

                Values.Add(new SqlParameter(ArgName, MemberValue));
                return ArgName;
            }

            // MethodCallExpression
            if (Expression is MethodCallExpression MCE)
                return ParseMethod(Parameter, MCE, ref Values);

            // BinaryExpression
            if (Expression is BinaryExpression BE &&
                typeof(bool).Equals(BE.Type))
            {
                string Left = ParseExpression(Parameter, BE.Left, ref Values),
                       Right = ParseExpression(Parameter, BE.Right, ref Values);

                switch (BE.NodeType)
                {
                    case ExpressionType.Equal:
                        return $"{Left}={Right}";
                    case ExpressionType.NotEqual:
                        return $"{Left}!={Right}";
                    case ExpressionType.And:
                    case ExpressionType.AndAlso:
                        return $"{Left} And {Right}";
                    case ExpressionType.Or:
                    case ExpressionType.OrAssign:
                        return $"({Left}) Or ({Right})";
                    case ExpressionType.GreaterThan:
                        return $"{Left}>{Right}";
                    case ExpressionType.GreaterThanOrEqual:
                        return $"{Left}>={Right}";
                    case ExpressionType.LessThan:
                        return $"{Left}<{Right}";
                    case ExpressionType.LessThanOrEqual:
                        return $"{Left}<={Right}";
                }
            }

            throw new NotSupportedException();
        }
        private static string ParseMethod(ParameterExpression Parameter, MethodCallExpression Method, ref List<SqlParameter> Values)
        {
            if (!(Method.Object is MemberExpression Param && Param.Expression.Equals(Parameter)) &&
                !Method.Arguments.Any(i => i is MemberExpression Member && Member.Expression.Equals(Parameter)))
            {
                object MethodValue = Method.GetValue();
                if (MethodValue is IEnumerable Enumerable)
                    return ParseEnumerable(Enumerable, ref Values);

                string ArgName = $"@Arg{Values.Count}";
                Values.Add(new SqlParameter(ArgName, MethodValue));
                return ArgName;
            }

            if (Method.Object is null)
            {
                if (Method.Arguments.Count.Equals(2) &&
                    Method.Arguments[0] is MethodCallExpression ExtensionMethod &&
                    Method.Arguments[1] is MemberExpression ME &&
                    ME.Expression.Equals(Parameter))
                    return $"({ME.Member.Name} In {ParseMethod(Parameter, ExtensionMethod, ref Values)})";

                throw new NotSupportedException();
            }

            switch (Method.Method.Name)
            {
                case "Equals":
                    return $"({ParseExpression(Parameter, Method.Object, ref Values)}={ParseExpression(Parameter, Method.Arguments[0], ref Values)})";
                case "Contains":
                    {
                        if (Method.Arguments.Count.Equals(1) &&
                            Method.Arguments[0] is MemberExpression ME &&
                            ME.Expression.Equals(Parameter) &&
                            Method.Object?.GetValue() is IEnumerable Enumerable)
                            return $"({ME.Member.Name} In {ParseEnumerable(Enumerable, ref Values)})";
                        break;
                    }
            }

            throw new NotSupportedException();
        }
        private static string ParseEnumerable(IEnumerable ArrayValue, ref List<SqlParameter> Values)
        {
            IEnumerator Enumerator = ArrayValue.GetEnumerator();
            List<string> Args = new List<string>();
            while (Enumerator.MoveNext())
            {
                string ArgName = $"@Arg{Values.Count}";
                Values.Add(new SqlParameter(ArgName, Enumerator.Current));
                Args.Add(ArgName);
            }

            return $"({string.Join(", ", Args)})";
        }

    }
}
