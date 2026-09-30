#if (MongoDB)
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

#endif
namespace TwinboxApp;

public sealed record PlaceOrder(decimal Total);

#if (MongoDB)
public sealed record Order([property: BsonGuidRepresentation(GuidRepresentation.Standard)] Guid Id, decimal Total);
#else
public sealed record Order(Guid Id, decimal Total);
#endif

public sealed record OrderPlaced(Guid OrderId, decimal Total);
