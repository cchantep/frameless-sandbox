# Frameless Sandbox

[Frameless](https://github.com/typelevel/frameless) sandbox.

## Usage

    sbt testOnly

Use cases failing with Frameless:

    sbt 'testOnly -- include ko'

Use case ok (with workaround of not):

    sbt 'testOnly -- include ok'