using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Twinbox.Inbox;
using Twinbox.Messaging;
using Twinbox.Serialization;
using Twinbox.Transport;

namespace Twinbox;

public sealed class TwinboxBuilder
{
    internal TwinboxBuilder(IServiceCollection services, RouteTable routes, MessageTypeRegistry messageTypes)
    {
        Services = services;
        Routes = routes;
        MessageTypes = messageTypes;
    }

    public IServiceCollection Services { get; }

    internal RouteTable Routes { get; }

    internal MessageTypeRegistry MessageTypes { get; }

    public RouteBuilder<TMessage> Route<TMessage>()
        where TMessage : class => new(this);

    /// <summary>The consumer name is the handler's inbox identity; keep it stable across renames to preserve deduplication.</summary>
    public TwinboxBuilder AddHandler<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler, TMessage>(string? consumerName = null)
        where THandler : class, IHandle<TMessage>
        where TMessage : class
    {
        MessageTypes.GetOrAdd(typeof(TMessage));
        Services.TryAddScoped<THandler>();
        Services.AddSingleton<HandlerDescriptor>(
            new HandlerDescriptor<THandler, TMessage>(consumerName ?? ConsumerNames.For(typeof(THandler))));
        return this;
    }

    /// <summary>The consumer name is the handler's inbox identity; keep it stable across renames to preserve deduplication.</summary>
    public TwinboxBuilder AddBatchHandler<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler, TMessage>(string? consumerName = null)
        where THandler : class, IHandleBatch<TMessage>
        where TMessage : class
    {
        MessageTypes.GetOrAdd(typeof(TMessage));
        Services.TryAddScoped<THandler>();
        Services.AddSingleton<HandlerDescriptor>(
            new BatchHandlerDescriptor<THandler, TMessage>(consumerName ?? ConsumerNames.For(typeof(THandler))));
        return this;
    }

    /// <summary>Registers <typeparamref name="THandler"/> for every <see cref="IHandle{TMessage}"/> it implements.</summary>
    [RequiresDynamicCode("Closes generic handler types at runtime. Use AddHandler<THandler, TMessage>() for Native AOT.")]
    [RequiresUnreferencedCode("Closes generic handler types at runtime. Use AddHandler<THandler, TMessage>() for trimmed apps.")]
    public TwinboxBuilder AddHandler<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces | DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(string? consumerName = null)
        where THandler : class
    {
        var messageTypes = typeof(THandler).GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IHandle<>))
            .Select(i => i.GetGenericArguments()[0])
            .ToArray();

        if (messageTypes.Length == 0)
        {
            throw new ArgumentException($"{typeof(THandler)} does not implement IHandle<TMessage>.", nameof(THandler));
        }

        Services.TryAddScoped<THandler>();
        foreach (var messageType in messageTypes)
        {
            MessageTypes.GetOrAdd(messageType);
            var descriptorType = typeof(HandlerDescriptor<,>).MakeGenericType(typeof(THandler), messageType);
            var descriptor = (HandlerDescriptor)Activator.CreateInstance(
                descriptorType, consumerName ?? ConsumerNames.For(typeof(THandler)))!;
            Services.AddSingleton(descriptor);
        }

        return this;
    }

    /// <summary>
    /// Adds the "local" transport: messages routed to it are delivered to this app's own handlers by the dispatcher,
    /// giving durable, retried in-process events.
    /// </summary>
    public TwinboxBuilder UseLocalDelivery()
    {
        Services.TryAddEnumerable(ServiceDescriptor.Singleton<ITransport, LocalTransport>(sp => new LocalTransport(sp.GetRequiredService<IInboundPipeline>())));
        return this;
    }

    /// <summary>Also writes and reads another system's header names; see <see cref="HeaderProfile"/>.</summary>
    /// <summary>Runs <typeparamref name="TFilter"/> around every handler call; filters run in registration order.</summary>
    public TwinboxBuilder AddFilter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TFilter>()
        where TFilter : class, IMessageFilter
    {
        Services.AddScoped<IMessageFilter, TFilter>();
        return this;
    }

    public TwinboxBuilder AddOutgoingFilter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TFilter>()
        where TFilter : class, IOutgoingMessageFilter
    {
        Services.AddScoped<IOutgoingMessageFilter, TFilter>();
        return this;
    }

    public TwinboxBuilder UseHeaderProfile(HeaderProfile profile)
    {
        ArgumentNullException.ThrowIfNull(profile);
        Services.AddSingleton(profile);
        return this;
    }

    public TwinboxBuilder UseTenants(Action<TenancyOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var tenancy = new TenancyOptions();
        configure(tenancy);
        if (tenancy.ListTenants is null || tenancy.EnterTenant is null || tenancy.CurrentTenant is null)
        {
            throw new ArgumentException("UseTenants needs ListTenants, EnterTenant and CurrentTenant.", nameof(configure));
        }

        Services.AddSingleton(tenancy);
        return this;
    }

    public TwinboxBuilder Configure(Action<TwinboxOptions> configure)
    {
        Services.Configure(configure);
        return this;
    }

    public TwinboxBuilder UseSerializer(IMessageSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        Services.Replace(ServiceDescriptor.Singleton(serializer));
        return this;
    }
}

public sealed class RouteBuilder<TMessage>
    where TMessage : class
{
    private readonly TwinboxBuilder _builder;

    internal RouteBuilder(TwinboxBuilder builder)
    {
        _builder = builder;
    }

    /// <summary>
    /// Sends <typeparamref name="TMessage"/> to a destination; route again to fan out. The transport name is only
    /// required when more than one transport is registered.
    /// </summary>
    public TwinboxBuilder To(string destination, string? transport = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);
        _builder.MessageTypes.GetOrAdd(typeof(TMessage));
        _builder.Routes.Add(typeof(TMessage), new Route(destination, transport));
        return _builder;
    }
}
