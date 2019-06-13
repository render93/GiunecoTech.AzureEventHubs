using System;

namespace EventHubs
{
    public class Device
    {
        public Sender Sender { get; set; }
        public string IpAddress { get; set; }
        public bool IsOnline { get; set; }
    }

    public enum Sender
    {
        Pc = 1,
        Mac = 2,
        Mobile = 3
    }
}
