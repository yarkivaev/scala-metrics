# Metrics DSL

[![CI](https://github.com/yarkivaev/metrics-dsl/actions/workflows/ci.yml/badge.svg)](https://github.com/yarkivaev/metrics-dsl/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A functional, type-safe Scala 3 library for defining, evaluating, and visualizing metrics using a declarative DSL.

## Features

- **Declarative DSL** - Define metrics using composable formulas with operator overloading
- **Type-safe** - Fully typed formulas with compile-time safety
- **Effect polymorphic** - Generic over effect types (IO, Try, Future)
- **Streaming** - Built on fs2 for efficient time-series processing
- **Extensible** - Plugin architecture for data sources and interpreters

## Installation

Add to your `build.sbt`:

```scala
libraryDependencies ++= Seq(
  "io.github.yarkivaev" %% "metrics-dsl-core" % "0.1.0",
  "io.github.yarkivaev" %% "metrics-dsl-runtime" % "0.1.0"
)
```

## Quick Example

```scala
import metrics.core.*
import metrics.dsl.FormulaDSL.{given, *}
import metrics.datasources.InMemoryDataSource
import metrics.interpreters.LocalFormulaInterpreter
import cats.effect.IO
import cats.effect.unsafe.implicits.global

// Create data source
val dataSource = InMemoryDataSource.builder[IO]()
  .addValue("revenue", 100000.0)
  .addValue("cost", 75000.0)
  .build()

// Define formulas
val revenue = Formula.QueryRef[IO, Double](Query.ByName("revenue"))
val cost = Formula.QueryRef[IO, Double](Query.ByName("cost"))
val profit = revenue - cost
val margin = (profit / revenue) * lit(100.0)

// Evaluate
val interpreter = new LocalFormulaInterpreter[IO, Double]()
val result = interpreter.eval(margin, dataSource).compile.last.unsafeRunSync()
// result: Some(25.0)
```

## Modules

- **metrics-dsl-core** - Pure DSL with minimal dependencies
- **metrics-dsl-runtime** - Interpreters and data source implementations

## Requirements

- Scala 3.3.1+
- Java 11+
- SBT 1.9+

## License

MIT License - see [LICENSE](LICENSE) file for details
