using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Tokens.CriticalSections
{
    internal class CriticalSectionState
    {
        private bool _isGranted;
        public bool IsGranted
        {
            get
            {
                return _isGranted;
            }
            set
            {
                if (_isGranted != value)
                    HasBeenModified = true;

                _isGranted = value;
            }
        }

        private string _grantedToExecution;
        public string GrantedToExecution
        {
            get
            {
                return _grantedToExecution;
            }
            set
            {
                if (_grantedToExecution != value)
                    HasBeenModified = true;

                _grantedToExecution = value;
            }
        }

        public bool HasBeenModified { get; private set; }

        private List<CriticalSectionQueueItem> _queue;

        public void StartTrackingModifications()
        {
            HasBeenModified = false;
        }

        public bool HasQueuedExecutions()
        {
            return _queue != null && _queue.Any();
        }

        public void SetQueue(string queueStr)
        {
            _queue = new List<CriticalSectionQueueItem>();
            if (!string.IsNullOrEmpty(queueStr))
            {
                var queueItems = queueStr.Split(new char[] { '|' }, StringSplitOptions.RemoveEmptyEntries);
                foreach (var queueItem in queueItems)
                {
                    var parts = queueItem.Split(',');
                    int index = int.Parse(parts[0]);
                    _queue.Add(new CriticalSectionQueueItem(index, parts[1]));
                }
            }
        }

        public string GetQueueString()
        {
            var sb = new StringBuilder();
            foreach (var queueItem in _queue)
            {
                if (queueItem.Index > 1)
                    sb.Append("|");

                sb.Append(queueItem.Index + "," + queueItem.TaskExecutionId);
            }

            return sb.ToString();
        }

        public List<CriticalSectionQueueItem> GetQueue()
        {
            return _queue;
        }

        public void UpdateQueue(List<CriticalSectionQueueItem> queueDetails)
        {
            _queue = queueDetails;
            HasBeenModified = true;
        }

        public string GetFirstExecutionIdInQueue()
        {
            if (_queue == null || !_queue.Any())
                return string.Empty;

            return _queue.OrderBy(x => x.Index).First().TaskExecutionId;
        }

        public void RemoveFirstInQueue()
        {
            // remove the first element
            if (_queue != null && _queue.Any())
                _queue.RemoveAt(0);

            // reset the index values
            int index = 1;
            foreach (var item in _queue.OrderBy(x => x.Index))
            {
                item.Index = index;
                index++;
            }

            HasBeenModified = true;
        }

        public bool ExistsInQueue(string taskExecutionId)
        {
            return _queue.Any(x => x.TaskExecutionId == taskExecutionId);
        }

        public void AddToQueue(string taskExecutionId)
        {
            int index = 1;
            if (_queue.Any())
                index = _queue.Max(x => x.Index) + 1;

            _queue.Add(new CriticalSectionQueueItem(index, taskExecutionId));
            HasBeenModified = true;
        }
    }
}
