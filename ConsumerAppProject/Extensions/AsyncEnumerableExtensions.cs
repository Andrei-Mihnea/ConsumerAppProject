using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Extensions
{
    public static class AsyncEnumerableExtensions
    {
        public static async IAsyncEnumerable<(T first, T second)> SlidingPairs<T>(
            this IAsyncEnumerable<T> source,
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            await using var enumerator = source.GetAsyncEnumerator(ct);

            if (!await enumerator.MoveNextAsync()) yield break;

            var prev = enumerator.Current;

            while(await enumerator.MoveNextAsync())
            {
                var curr = enumerator.Current;
                yield return (prev, curr);
                prev = curr;
            }
        }
    }
}
