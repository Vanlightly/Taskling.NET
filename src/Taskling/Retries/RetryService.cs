using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Taskling.Exceptions;

namespace Taskling.Retries
{
    public class RetryService
    {
        public static async Task InvokeWithRetryAsync<RQ>(Func<RQ, Task> requestAction, RQ request)
        {
            int interval = 5000;
            double publishExponentialBackoffExponent = 2;
            int attemptLimit = 3;
            int attemptsMade = 0;
            
            bool successFullySent = false;
            Exception lastException = null;

            while (attemptsMade < attemptLimit && successFullySent == false)
            {
                try
                {
                    await requestAction(request);
                    successFullySent = true;
                }
                catch (TransientException ex)
                {
                    lastException = ex;
                }

                interval = (int)(interval * publishExponentialBackoffExponent);
                attemptsMade++;

                if (!successFullySent)
                    await Task.Delay(interval);
            }

            if (!successFullySent)
            {
                throw new ExecutionException("A persistent transient exception has caused all retries to fail", lastException);
            }
        }
    }
}
