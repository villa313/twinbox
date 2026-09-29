namespace Twinbox.UnitTests;

public sealed record OrderPlaced(int OrderId);

[MessageName("invoice-issued.v1")]
public sealed record InvoiceIssued(int InvoiceId);

public sealed record OrderShipped(int OrderId);

public abstract record DomainEvent;

public sealed record CustomerRegistered(int CustomerId) : DomainEvent;

public interface IAuditEvent;

public sealed record SettingChanged(string Key) : IAuditEvent;
