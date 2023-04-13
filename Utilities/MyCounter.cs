using System.Diagnostics;

namespace Utilities
{
    public class MyCounter
    {
        int count;

        public MyCounter() => count = 0;
        
        public void Increment() => count++;

        public bool Decrement()
        {
            Debug.Assert(count > 0);
            count--;
            return count == 0;
        } 

        public int GetCount() => count;
    }
}