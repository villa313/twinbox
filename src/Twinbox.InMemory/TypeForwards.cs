using System.Runtime.CompilerServices;

// These types moved into the Twinbox package; forwarding keeps apps compiled against Twinbox.InMemory 1.0 working.
[assembly: TypeForwardedTo(typeof(Twinbox.InMemory.InMemoryInboxStore))]
[assembly: TypeForwardedTo(typeof(Twinbox.InMemory.InMemoryOutboxStore))]
[assembly: TypeForwardedTo(typeof(Twinbox.InMemory.InMemoryTransport))]
[assembly: TypeForwardedTo(typeof(Twinbox.InMemory.InMemoryTransportOptions))]
[assembly: TypeForwardedTo(typeof(Twinbox.InMemory.InMemoryUnitOfWork))]
[assembly: TypeForwardedTo(typeof(Twinbox.InMemoryTwinboxBuilderExtensions))]
