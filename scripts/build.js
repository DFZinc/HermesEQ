const { execSync } = require('child_process');
const fs = require('fs-extra');
const path = require('path');
const os = require('os');
const rimraf = require('rimraf');

const PYTHON_VERSION = '3.11';
const PLATFORM = process.platform;
const ARCH = process.arch;

// Paths
const ROOT_DIR = path.resolve(__dirname, '..');
const PYTHON_APP_DIR = path.join(ROOT_DIR, 'python-app');
const BUILD_DIR = path.join(ROOT_DIR, 'build');
const PYTHON_BUNDLE_DIR = path.join(BUILD_DIR, 'python');

// Ensure Python is installed
function checkPython() {
  try {
    const output = execSync('python3 --version', { encoding: 'utf8' });
    const version = output.match(/\d+\.\d+/)[0];
    console.log(`Found Python ${version}`);
    return true;
  } catch (err) {
    console.error('Python 3 not found. Please install Python 3.8 or higher.');
    return false;
  }
}

// Install Python dependencies
function installPythonDeps() {
  console.log('Installing Python dependencies...');
  const requirementsPath = path.join(PYTHON_APP_DIR, 'requirements.txt');
  
  if (!fs.existsSync(requirementsPath)) {
    console.log('No requirements.txt found, creating one...');
    const requirements = `
fastapi==0.104.1
uvicorn[standard]==0.24.0
aiohttp==3.9.1
openpyxl==3.1.2
python-multipart==0.0.6
    `.trim();
    fs.writeFileSync(requirementsPath, requirements);
  }
  
  execSync('pip3 install -r ' + requirementsPath, { 
    stdio: 'inherit',
    cwd: PYTHON_APP_DIR 
  });
}

// Bundle Python with PyInstaller
async function bundlePython() {
  console.log('Bundling Python application with PyInstaller...');
  
  // Ensure PyInstaller is installed
  try {
    execSync('pip3 show pyinstaller', { stdio: 'pipe' });
  } catch {
    console.log('Installing PyInstaller...');
    execSync('pip3 install pyinstaller', { stdio: 'inherit' });
  }
  
  // Clean previous build
  rimraf.sync(path.join(PYTHON_APP_DIR, 'dist'));
  rimraf.sync(path.join(PYTHON_APP_DIR, 'build'));
  rimraf.sync(PYTHON_BUNDLE_DIR);
  
  // Build with PyInstaller
  const pyInstallerArgs = [
    'pyinstaller',
    '--onefile',
    '--name', 'hermeseq-backend',
    '--distpath', path.join(PYTHON_APP_DIR, 'dist'),
    '--workpath', path.join(PYTHON_APP_DIR, 'build'),
    '--specpath', path.join(PYTHON_APP_DIR, 'build'),
    '--hidden-import', 'uvicorn',
    '--hidden-import', 'uvicorn.loops',
    '--hidden-import', 'uvicorn.loops.auto',
    '--hidden-import', 'uvicorn.protocols',
    '--hidden-import', 'uvicorn.protocols.http',
    '--hidden-import', 'uvicorn.protocols.http.auto',
    '--hidden-import', 'uvicorn.protocols.websockets',
    '--hidden-import', 'uvicorn.protocols.websockets.auto',
    '--collect-all', 'fastapi',
    '--collect-all', 'aiohttp',
    '--collect-all', 'openpyxl',
    'server.py'
  ];
  
  if (PLATFORM === 'win32') {
    pyInstallerArgs.push('--console');
  }
  
  execSync(pyInstallerArgs.join(' '), {
    stdio: 'inherit',
    cwd: PYTHON_APP_DIR,
    env: { ...process.env, PYTHONOPTIMIZE: '1' }
  });
  
  // Copy bundled executable to build directory
  const ext = PLATFORM === 'win32' ? '.exe' : '';
  const bundledExe = path.join(PYTHON_APP_DIR, 'dist', `hermeseq-backend${ext}`);
  const targetExe = path.join(PYTHON_BUNDLE_DIR, `hermeseq-backend${ext}`);
  
  fs.ensureDirSync(PYTHON_BUNDLE_DIR);
  fs.copyFileSync(bundledExe, targetExe);
  
  console.log(`Python bundled to: ${targetExe}`);
  
  // Copy requirements and config files
  fs.copySync(PYTHON_APP_DIR, path.join(BUILD_DIR, 'python-app'), {
    filter: (src) => {
      const exclude = ['__pycache__', '*.pyc', 'dist', 'build', '*.spec'];
      return !exclude.some(pattern => src.includes(pattern));
    }
  });
}

// Create requirements.txt if missing
function createRequirementsFile() {
  const reqPath = path.join(PYTHON_APP_DIR, 'requirements.txt');
  if (!fs.existsSync(reqPath)) {
    const content = `
fastapi==0.104.1
uvicorn[standard]==0.24.0
aiohttp==3.9.1
openpyxl==3.1.2
python-multipart==0.0.6
    `.trim();
    fs.writeFileSync(reqPath, content);
  }
}

// Main build process
async function build() {
  console.log('Building HermesEQ Desktop App...');
  console.log(`Platform: ${PLATFORM} (${ARCH})`);
  
  // Create requirements file if needed
  createRequirementsFile();
  
  // Check Python
  if (!checkPython()) {
    console.error('Build failed: Python not found');
    process.exit(1);
  }
  
  // Install dependencies
  installPythonDeps();
  
  // Bundle Python
  await bundlePython();
  
  // Copy static files
  const staticSrc = path.join(ROOT_DIR, 'static');
  const staticDest = path.join(BUILD_DIR, 'static');
  if (fs.existsSync(staticSrc)) {
    fs.copySync(staticSrc, staticDest);
  }
  
  console.log('Build complete!');
  console.log('Run "npm run dist" to create distributable packages');
}

// Run build
build().catch(err => {
  console.error('Build failed:', err);
  process.exit(1);
});