namespace Twinbox.Transport;

/// <summary>Header names transports use when a broker has no native property for the value.</summary>
public static class TransportHeaders
{
    public const string MessageId = "twinbox-message-id";
    public const string MessageName = "twinbox-message-name";
    public const string PartitionKey = "twinbox-partition-key";
    public const string TraceParent = "traceparent";
    public const string TraceState = "tracestate";
    public const string Baggage = "baggage";
    public const string TenantId = "twinbox-tenant";
    public const string DeliveryAttempt = "twinbox-delivery-attempt";
    public const string CorrelationId = "twinbox-correlation-id";
    public const string ReplyTo = "twinbox-reply-to";
}
