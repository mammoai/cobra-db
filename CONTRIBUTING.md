# Contributing

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given. Our main communication channel is
[Github Issues](https://github.com/mammoai/cobra-db/issues). The
[Markdown](https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax)
syntax is used for editing and formating.

## Types of Contributions

### Ask questions

Asking questions in Github Issues help us understand what are the uses that you have for
the tool. It also helps other users to find previously answered questions quickly.

If you are asking a question, please be as specific as possible.

### Report Bugs

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

### Fix Bugs

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

### Implement Features

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

### Write Documentation

You can never have enough documentation! Please feel free to contribute to any
part of the documentation, such as the official docs, docstrings, or even
on the web in blog posts, articles, and such.

### Submit Feedback

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

## Get Started!

Ready to contribute? Here's how to set up `cobra_db` for local development.

1. Create a fork of this repo, then clone it to your local.
2. __Optional__: create a conda environment:
    ```bash
    conda create -n cobra python=3.9
    conda activate cobra
    ```
3. `cd` to the forked repo and install the dev environment of `cobra_db` using `poetry`:

    ```bash
    poetry install
    ```

4. Make your modifications.

5. When you're done making changes, check that the tests are passing:
    ```bash
    poetry run pytest tests/ --cov=cobra_db
    ## deselect slow tests with '-m "not slow"'
    # poetry run pytest tests/ --cov=cobra_db -m "not slow"
    ```

    If you made changes to the documentation, you can build it locally
    ```bash
    cd docs
    make html -B
    ```

6. Pre-commit hooks are configured for style: isort, black and flake8. You can manually
   run them by staging your changes and then:
    ```bash
    pre-commit run --all-files
    ```

7. Commit your changes following the
   [Angular Commit Message Format](https://gist.github.com/brianclements/841ea7bffdb01346392c)
   and open a pull request. Following the Angular message format, allows us to
   automaticallykeep track of the versions and the change log. A quick recap of the
   format:
    ```
    <type>(<scope>): <subject>
    <BLANK LINE>
    <body>
    <BLANK LINE>
    <footer>
    ```
    Type must be one of the following:
    * **build**: Changes that affect the build system or external dependencies
    * **ci**: Changes to our CI configuration files and scripts (github actions
      workflows)
    * **docs**: Documentation only changes
    * **feat**: A new feature
    * **fix**: A bug fix
    * **perf**: A code change that improves performance
    * **refactor**: A code change that neither fixes a bug nor adds a feature
    * **style**: Changes that do not affect the meaning of the code (white-space,
      formatting, missing semi-colons, etc)
    * **test**: Adding missing tests or correcting existing tests

### Scope
The scope should be the name of the npm package affected (as perceived by the person
reading the changelog generated from commit messages).


## Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include additional tests if appropriate.
2. If the pull request adds functionality, the docs should be updated.
3. The pull request should work for all currently supported operating systems and
   versions of Python.
4. The commit messages are in the [Angular Commit Message Format](https://gist.github.com/brianclements/841ea7bffdb01346392c)

## Code of Conduct

Please note that the `cobra_db` project is released with a
Code of Conduct. By contributing to this project you agree to abide by its terms.
