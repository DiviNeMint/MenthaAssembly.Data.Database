using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace MenthaAssembly.Data.Primitives
{
    public interface IDatabaseLinq<T>
    {
        Task<IEnumerable<T>> Query();

        IDatabaseLinq<T> Select(params Expression<Func<T, object>>[] Selectors);

        IDatabaseLinq<T> Where(Expression<Func<T, bool>> Predict);

        Task Remove();

    }
}
