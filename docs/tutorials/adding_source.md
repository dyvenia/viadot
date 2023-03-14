# Adding a source

## 1. Add a source
To add a source, create a new file in `viadot/sources`. The source must inherit from the `Source` base class and accept a `credentials` parameter.

## 2. Add unit tests
To add a unit tests, create a new file in `viadot/tests/unit`. The tests should cover each of the functionalities of the source.

## 3. Enter the changes in the CHANGELOG.md.

Add an appropriate description of the changes made to the sources in the `CHANGELOG.md` file and in the `mkdocs` documentation if, for example, you are adding a new source.
