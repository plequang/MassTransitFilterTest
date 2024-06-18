using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MassTransit;
using MassTransit.Configuration;
using MassTransit.Internals;
using MassTransit.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;
using NUnit.Framework;

namespace MassTransitFilterTest;

[TestFixture]
public class Test
{
    [Test]
    public async Task Filter_Test()
    {
        await using var provider = new ServiceCollection()
            .AddMassTransitTestHarness(x =>
            {
                x.AddConfigureEndpointsCallback((_, cfg) => cfg.UseMessageFilter());
                x.AddConsumer<MessageConsumer>();
            })
            .BuildServiceProvider(true);

        var harness = provider.GetRequiredService<ITestHarness>();

        await harness.Start();

        await harness.Bus.Publish(new Message() { Text = "Message 1" });

        var messages = await harness.Consumed.SelectAsync<Message>().ToListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(messages.Count, Is.EqualTo(1));
            Assert.That(messages[0].Context.Message.FromFilter, Is.EqualTo("FromFilter"));
        });
    }

    [Test]
    public async Task Batch_With_Filter_Test()
    {
        await using var provider = new ServiceCollection()
            .AddMassTransitTestHarness(x =>
            {
                x.AddConfigureEndpointsCallback((_, cfg) => cfg.UseMessageFilter());
                x.AddConsumer<BatchMessageConsumer>(cfg =>
                {
                    cfg.Options<BatchOptions>(options => options
                        .SetMessageLimit(2));
                });
            })
            .BuildServiceProvider(true);

        var harness = provider.GetRequiredService<ITestHarness>();

        await harness.Start();

        var consumerHarness = harness.GetConsumerHarness<BatchMessageConsumer>();

        await harness.Bus.PublishBatch(new[]
        {
            new Message() { Text = "Message 1" },
            new Message() { Text = "Message 2" },
        });

        var messages = await harness.Consumed.SelectAsync<Message>().ToListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(messages.Count, Is.EqualTo(2));

            Assert.That(messages, Has.All.Matches<IReceivedMessage<Message>>(m => m.Context.Message.FromFilter == "FromFilter"));
        });
    }
}

public record Message
{
    public string Text { get; set; }

    public string FromFilter { get; set; }
}

public class BatchMessageConsumer :
    IConsumer<Batch<Message>>
{
    public Task Consume(ConsumeContext<Batch<Message>> context)
    {
        for (int i = 0; i < context.Message.Length; i++)
        {
            ConsumeContext<Message> message = context.Message[i];
            Console.WriteLine($"{message.Message.Text} {message.Message.FromFilter}");
        }

        return Task.CompletedTask;
    }
}

public class MessageConsumer : IConsumer<Message>
{
    public Task Consume(ConsumeContext<Message> context)
    {
        Console.WriteLine($"{context.Message.Text} {context.Message.FromFilter}");
        return Task.CompletedTask;
    }
}

public class MessageFilterConfigurationObserver :
    ConfigurationObserver,
    IMessageConfigurationObserver
{
    public MessageFilterConfigurationObserver(IConsumePipeConfigurator receiveEndpointConfigurator)
        : base(receiveEndpointConfigurator)
    {
        Connect(this);
    }

    public void MessageConfigured<TMessage>(IConsumePipeConfigurator configurator)
        where TMessage : class
    {
        // (pasleq) This is never called for type Message when 
        var specification = new MessageFilterPipeSpecification<TMessage>();

        configurator.AddPipeSpecification(specification);
    }
}

public class MessageFilterPipeSpecification<T> :
    IPipeSpecification<ConsumeContext<T>>
    where T : class
{
    public void Apply(IPipeBuilder<ConsumeContext<T>> builder)
    {
        var filter = new MessageFilter<T>();

        builder.AddFilter(filter);
    }

    public IEnumerable<ValidationResult> Validate()
    {
        yield break;
    }
}

public class MessageFilter<T> :
    IFilter<ConsumeContext<T>>
    where T : class
{
    public void Probe(ProbeContext context)
    {
        var scope = context.CreateFilterScope("messageFilter");
    }

    public async Task Send(ConsumeContext<T> context, IPipe<ConsumeContext<T>> next)
    {
        // do something
        if (context.Message is Message message)
        {
            message.FromFilter = "FromFilter";
        }

        await next.Send(context);
    }
}

public static class MessageFilterConfigurationExtensions
{
    public static void UseMessageFilter(this IConsumePipeConfigurator configurator)
    {
        if (configurator == null)
            throw new ArgumentNullException(nameof(configurator));

        var observer = new MessageFilterConfigurationObserver(configurator);
    }
}