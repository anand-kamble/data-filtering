## Initial Setup of Repository


Below are the repository configs.

1. Package Manager
    * Poetry v1.8.2

2. Branch Rules
    * `main` branch is protected and commits cannot be directly pushed to this branch.

3. VS-Code Settings
    * `.vscode` folder includes the settings.json file which specify the Workspace settings.  
    You can find out more about Workspace settings [here](https://code.visualstudio.com/docs/getstarted/settings#_workspace-settings)

4. [Commitizen](https://commitizen-tools.github.io/commitizen/)
    * This tool is used to manage commits.
    * It is having some trouble to be installed using poetry. So I have added it using pip inside the poetry shell.
    * Using pep440 version scheme.
    * This will manage all the commits and also bump up the version of the package.
    * Using command 'cz ch' a changelog can also be generated automatically which will be stored in 'CHANGELOG.md' file.
