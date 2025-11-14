package metrics.interpreters

/**
 * Generic property extraction using Java reflection
 *
 * Works with any case class without requiring domain-specific code.
 * Uses reflection to access fields at runtime, maintaining the
 * domain-agnostic nature of the core library.
 */
object PropertyExtractor:

  /**
   * Extract a property value from any object using reflection
   *
   * @param obj The object to extract from (typically a case class instance)
   * @param propertyName Name of the field to extract
   * @tparam A The expected type of the property value
   * @return The property value cast to type A
   * @throws RuntimeException if property not found or type mismatch
   */
  def extract[A](obj: Any, propertyName: String): A =
    // Get all declared fields from the object's class
    val fields = obj.getClass.getDeclaredFields

    // Find the field with matching name
    fields.find(_.getName == propertyName) match {
      case Some(field) =>
        // Make field accessible (in case it's private)
        field.setAccessible(true)

        // Get the value and cast to expected type
        try
          field.get(obj).asInstanceOf[A]
        catch {
          case e: ClassCastException =>
            throw new RuntimeException(
              s"Property '$propertyName' on ${obj.getClass.getSimpleName} " +
                s"cannot be cast to expected type: ${e.getMessage}",
            )
        }

      case None =>
        // Property not found - provide helpful error message
        val availableFields = fields.map(_.getName).mkString(", ")
        throw new RuntimeException(
          s"Property '$propertyName' not found on ${obj.getClass.getSimpleName}. " +
            s"Available properties: $availableFields",
        )
    }
