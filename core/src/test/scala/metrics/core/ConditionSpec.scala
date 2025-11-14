package metrics.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConditionSpec extends AnyFlatSpec with Matchers:
  type Id[A] = A

  "Condition.GreaterThan" should "create a greater than comparison" in {
    val left      = Formula.Literal[Id, Double](10.0)
    val right     = Formula.Literal[Id, Double](5.0)
    val condition = Condition.GreaterThan[Id, Double](left, right)

    condition.left should be(left)
    condition.right should be(right)
  }

  "Condition.LessThan" should "create a less than comparison" in {
    val left      = Formula.Literal[Id, Double](5.0)
    val right     = Formula.Literal[Id, Double](10.0)
    val condition = Condition.LessThan[Id, Double](left, right)

    condition.left should be(left)
    condition.right should be(right)
  }

  "Condition.Equals" should "create an equality comparison" in {
    val left      = Formula.Literal[Id, Double](42.0)
    val right     = Formula.Literal[Id, Double](42.0)
    val condition = Condition.Equals[Id, Double](left, right)

    condition.left should be(left)
    condition.right should be(right)
  }

  "Condition.And" should "create a logical AND with multiple conditions" in {
    val cond1 = Condition.GreaterThan[Id, Double](Formula.Literal[Id, Double](10.0), Formula.Literal[Id, Double](5.0))
    val cond2 = Condition.LessThan[Id, Double](Formula.Literal[Id, Double](10.0), Formula.Literal[Id, Double](20.0))
    val condition = Condition.And[Id, Double](List(cond1, cond2))

    condition.conditions should have length 2
    condition.conditions should contain(cond1)
    condition.conditions should contain(cond2)
  }

  it should "support empty condition list" in {
    val condition = Condition.And[Id, Double](List.empty)
    condition.conditions should be(empty)
  }

  it should "support single condition" in {
    val cond = Condition.GreaterThan[Id, Double](Formula.Literal[Id, Double](10.0), Formula.Literal[Id, Double](5.0))
    val condition = Condition.And[Id, Double](List(cond))
    condition.conditions should have length 1
  }

  "Condition.Or" should "create a logical OR with multiple conditions" in {
    val cond1 = Condition.GreaterThan[Id, Double](Formula.Literal[Id, Double](10.0), Formula.Literal[Id, Double](5.0))
    val cond2 = Condition.LessThan[Id, Double](Formula.Literal[Id, Double](3.0), Formula.Literal[Id, Double](2.0))
    val condition = Condition.Or[Id, Double](List(cond1, cond2))

    condition.conditions should have length 2
    condition.conditions should contain(cond1)
    condition.conditions should contain(cond2)
  }

  it should "support empty condition list" in {
    val condition = Condition.Or[Id, Double](List.empty)
    condition.conditions should be(empty)
  }

  "Condition.Not" should "create a logical NOT" in {
    val innerCondition =
      Condition.GreaterThan[Id, Double](Formula.Literal[Id, Double](10.0), Formula.Literal[Id, Double](5.0))
    val condition      = Condition.Not[Id, Double](innerCondition)

    condition.condition should be(innerCondition)
  }

  "Nested conditions" should "support complex logical expressions" in {
    val cond1 = Condition.GreaterThan[Id, Double](Formula.MetricRef[Id, Double]("a"), Formula.Literal[Id, Double](5.0))
    val cond2 = Condition.LessThan[Id, Double](Formula.MetricRef[Id, Double]("b"), Formula.Literal[Id, Double](10.0))
    val andCondition = Condition.And[Id, Double](List(cond1, cond2))

    andCondition.isInstanceOf[Condition.And[?, ?]] should be(true)
    andCondition.conditions should have length 2
  }

  it should "support NOT of compound conditions" in {
    val cond1 = Condition.GreaterThan[Id, Double](Formula.MetricRef[Id, Double]("a"), Formula.Literal[Id, Double](5.0))
    val cond2 = Condition.LessThan[Id, Double](Formula.MetricRef[Id, Double]("b"), Formula.Literal[Id, Double](10.0))
    val andCondition = Condition.And[Id, Double](List(cond1, cond2))
    val notCondition = Condition.Not[Id, Double](andCondition)

    notCondition.isInstanceOf[Condition.Not[?, ?]] should be(true)
    notCondition.condition.isInstanceOf[Condition.And[?, ?]] should be(true)
  }

  it should "support deeply nested logical expressions" in {
    val cond1 = Condition.GreaterThan[Id, Double](Formula.MetricRef[Id, Double]("a"), Formula.Literal[Id, Double](5.0))
    val cond2 = Condition.LessThan[Id, Double](Formula.MetricRef[Id, Double]("b"), Formula.Literal[Id, Double](10.0))
    val cond3 = Condition.Equals[Id, Double](Formula.MetricRef[Id, Double]("c"), Formula.Literal[Id, Double](0.0))

    val andCondition = Condition.And[Id, Double](List(cond1, cond2))
    val notCondition = Condition.Not[Id, Double](cond3)
    val orCondition  = Condition.Or[Id, Double](List(andCondition, notCondition))

    orCondition.isInstanceOf[Condition.Or[?, ?]] should be(true)
    orCondition.conditions should have length 2
    orCondition.conditions(0).isInstanceOf[Condition.And[?, ?]] should be(true)
    orCondition.conditions(1).isInstanceOf[Condition.Not[?, ?]] should be(true)
  }

  it should "support multiple levels of nesting" in {
    val baseCondition =
      Condition.GreaterThan[Id, Double](Formula.MetricRef[Id, Double]("a"), Formula.Literal[Id, Double](5.0))
    val not1          = Condition.Not[Id, Double](baseCondition)
    val not2          = Condition.Not[Id, Double](not1)
    val not3          = Condition.Not[Id, Double](not2)

    not3.isInstanceOf[Condition.Not[?, ?]] should be(true)
    not3.condition.isInstanceOf[Condition.Not[?, ?]] should be(true)
    not3.condition.asInstanceOf[Condition.Not[Id, Double]].condition.isInstanceOf[Condition.Not[?, ?]] should be(true)
  }
