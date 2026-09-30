namespace Twinbox;

public enum OutboxMessageStatus
{
    Pending = 0,
    Processing = 1,
    Sent = 2,
    Dead = 3,
}
