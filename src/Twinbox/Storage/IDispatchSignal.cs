namespace Twinbox.Storage;

/// <summary>Stores call this after a commit so the dispatcher sends immediately instead of waiting for the next poll.</summary>
public interface IDispatchSignal
{
    void Notify();
}
