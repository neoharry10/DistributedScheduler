namespace DistributedScheduler.Domain;

public record InvoicePayload(Guid CustomerId, decimal Amount);