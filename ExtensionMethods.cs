namespace Caligo.SqlUpdateScriptGenerator {
  public static class ExtensionMethods {
    public static bool IsLast<T>(this IList<T> list, int index)
    {
        if (list == null)
        {
            throw new ArgumentNullException(nameof(list));
        }

        if (index < 0 || index >= list.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");
        }

        return index == list.Count - 1;
    }
  }
}