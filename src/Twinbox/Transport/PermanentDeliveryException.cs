namespace Twinbox.Transport;

public sealed class PermanentDeliveryException : Exception
{
    public PermanentDeliveryException()
    {
    }

    public PermanentDeliveryException(string message)
        : base(message)
    {
    }

    public PermanentDeliveryException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
