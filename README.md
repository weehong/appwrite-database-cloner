# appwrite-database-cloner

A software project designed to clone databases in Appwrite, enabling developers to easily replicate their database environments for testing and development purposes.

## Features

- **Environment Configuration**: An example environment file `.env.example` to help configure your local environment.
- **Project Structure**: A simple structure with main entry point in `index.js`.
- **Development Tools**: Includes scripts for starting, developing, linting, and testing the application.
- **Linting**: Automatically check for code quality using ESLint to maintain code standards.

## Installation Instructions

To get started with the `appwrite-database-cloner`, follow these steps:

1. **Clone the repository**:
   ```bash
   git clone https://github.com/weehong/appwrite-database-cloner.git
   cd appwrite-database-cloner
   ```

2. **Install dependencies**:
   Make sure you have [Node.js](https://nodejs.org/) installed. Then, run:
   ```bash
   npm install
   ```

3. **Configure environment variables**:
   Copy the `.env.example` to `.env` and update the values as necessary:
   ```bash
   cp .env.example .env
   ```

## Usage Examples

To start the application, use the following command:
```bash
npm start
```

For development mode with automatic restarts on file changes, use:
```bash
npm run dev
```

To check the code quality, run:
```bash
npm run lint
```

To automatically fix linting issues, use:
```bash
npm run lint:fix
```

## Contributing Guidelines

We welcome contributions to this project! If you would like to contribute, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or fix:
   ```bash
   git checkout -b feature/YourFeature
   ```
3. Make your changes and commit them:
   ```bash
   git commit -m "Add some feature"
   ```
4. Push to your fork:
   ```bash
   git push origin feature/YourFeature
   ```
5. Open a pull request with a description of your changes.

Please ensure your code adheres to the existing coding standards and passes all linting checks.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.