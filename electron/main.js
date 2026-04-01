const { app, BrowserWindow, dialog, ipcMain } = require('electron');
const { spawn, spawnSync } = require('child_process');
const path = require('path');
const fs = require('fs');
const findFreePort = require('find-free-port');
const waitOn = require('wait-on');
const os = require('os');

// Store reference to Python process
let pythonProcess = null;
let mainWindow = null;
let serverPort = null;
let isQuitting = false;

// Get platform-specific Python executable path
function getPythonExecutable() {
  if (app.isPackaged) {
    // In packaged app, use bundled Python
    const platform = process.platform;
    const pythonDir = path.join(process.resourcesPath, 'python');
    
    if (platform === 'win32') {
      return path.join(pythonDir, 'python.exe');
    } else if (platform === 'darwin') {
      return path.join(pythonDir, 'bin', 'python3');
    } else {
      return path.join(pythonDir, 'bin', 'python3');
    }
  } else {
    // Development mode - use system Python
    return 'python3';
  }
}

// Get Python app directory
function getPythonAppDir() {
  if (app.isPackaged) {
    return path.join(process.resourcesPath, 'python-app');
  } else {
    return path.join(__dirname, '..', 'python-app');
  }
}

// Get static files directory
function getStaticDir() {
  if (app.isPackaged) {
    return path.join(process.resourcesPath, 'static');
  } else {
    return path.join(__dirname, '..', 'static');
  }
}

// Check if Python dependencies are installed
async function checkPythonDependencies(pythonExec, pythonDir) {
  return new Promise((resolve) => {
    const requirementsPath = path.join(pythonDir, 'requirements.txt');
    
    if (!fs.existsSync(requirementsPath)) {
      console.log('No requirements.txt found, skipping dependency check');
      resolve(true);
      return;
    }
    
    // Try to import fastapi
    const checkProcess = spawn(pythonExec, ['-c', 'import fastapi, uvicorn, aiohttp, openpyxl']);
    let stderr = '';
    
    checkProcess.stderr.on('data', (data) => {
      stderr += data.toString();
    });
    
    checkProcess.on('close', (code) => {
      if (code !== 0) {
        console.log('Missing Python dependencies. Installing...');
        const installProcess = spawn(pythonExec, ['-m', 'pip', 'install', '-r', requirementsPath]);
        
        installProcess.stdout.on('data', (data) => console.log(data.toString()));
        installProcess.stderr.on('data', (data) => console.error(data.toString()));
        
        installProcess.on('close', (installCode) => {
          resolve(installCode === 0);
        });
      } else {
        resolve(true);
      }
    });
  });
}

// Start Python FastAPI server
async function startPythonServer() {
  return new Promise(async (resolve, reject) => {
    const pythonExec = getPythonExecutable();
    const pythonDir = getPythonAppDir();
    
    console.log(`Using Python: ${pythonExec}`);
    console.log(`Python app dir: ${pythonDir}`);
    
    // Check if Python executable exists
    if (!fs.existsSync(pythonExec) && app.isPackaged) {
      dialog.showErrorBox(
        'Python Not Found',
        'The bundled Python runtime is missing. Please reinstall the application.'
      );
      reject(new Error('Python runtime not found'));
      return;
    }
    
    // Check dependencies
    const depsOk = await checkPythonDependencies(pythonExec, pythonDir);
    if (!depsOk) {
      dialog.showErrorBox(
        'Dependency Error',
        'Failed to install Python dependencies. Please check your internet connection and try again.'
      );
      reject(new Error('Dependency installation failed'));
      return;
    }
    
    // Find an available port
    findFreePort(8000, 8100, '127.0.0.1', (err, port) => {
      if (err) {
        reject(err);
        return;
      }
      
      serverPort = port;
      console.log(`Starting Python server on port ${serverPort}`);
      
      // Spawn Python process with uvicorn
      const args = [
        '-m', 'uvicorn',
        'server:app',
        '--host', '127.0.0.1',
        '--port', serverPort.toString(),
        '--log-level', 'warning'
      ];
      
      pythonProcess = spawn(pythonExec, args, {
        cwd: pythonDir,
        env: {
          ...process.env,
          PYTHONUNBUFFERED: '1',
          PYTHONPATH: pythonDir
        }
      });
      
      let startupOutput = '';
      let hasError = false;
      
      pythonProcess.stdout.on('data', (data) => {
        const output = data.toString();
        startupOutput += output;
        console.log(`Python stdout: ${output}`);
        
        // Check if server is ready
        if (output.includes('Application startup complete') || 
            output.includes('Uvicorn running')) {
          resolve(serverPort);
        }
      });
      
      pythonProcess.stderr.on('data', (data) => {
        const error = data.toString();
        console.error(`Python stderr: ${error}`);
        startupOutput += error;
        
        if (error.includes('Error') && !hasError) {
          hasError = true;
          reject(new Error(`Python startup error: ${error}`));
        }
      });
      
      pythonProcess.on('error', (err) => {
        console.error('Failed to start Python process:', err);
        reject(err);
      });
      
      pythonProcess.on('exit', (code) => {
        if (!isQuitting && code !== 0) {
          console.log(`Python process exited with code ${code}`);
          if (mainWindow && !mainWindow.isDestroyed()) {
            dialog.showErrorBox(
              'Server Error',
              `The Python backend stopped unexpectedly (exit code: ${code}). Please restart the application.`
            );
          }
        }
      });
      
      // Timeout after 30 seconds
      setTimeout(() => {
        if (!serverPort) {
          reject(new Error('Python server startup timeout (30s)'));
        }
      }, 30000);
    });
  });
}

// Create Electron window
async function createWindow() {
  const port = await startPythonServer();
  
  // Wait for server to be ready
  await waitOn({
    resources: [`http://127.0.0.1:${port}`],
    timeout: 30000,
    interval: 500
  });
  
  mainWindow = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 1024,
    minHeight: 768,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.js')
    },
    icon: path.join(getStaticDir(), 'icon.png'),
    show: false,
    backgroundColor: '#0a0a0f',
    titleBarStyle: process.platform === 'darwin' ? 'hiddenInset' : 'default',
    frame: true
  });
  
  // Load the app
  mainWindow.loadURL(`http://127.0.0.1:${port}`);
  
  // Show window when ready
  mainWindow.once('ready-to-show', () => {
    mainWindow.show();
    
    // Open DevTools in development
    if (!app.isPackaged) {
      mainWindow.webContents.openDevTools();
    }
  });
  
  // Handle external links
  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    require('electron').shell.openExternal(url);
    return { action: 'deny' };
  });
  
  // Handle window close
  mainWindow.on('close', (event) => {
    if (!isQuitting) {
      event.preventDefault();
      isQuitting = true;
      mainWindow.webContents.send('app-quit');
    }
  });
}

// Graceful shutdown
async function shutdown() {
  isQuitting = true;
  
  if (pythonProcess && !pythonProcess.killed) {
    console.log('Shutting down Python server...');
    pythonProcess.kill();
    
    // Give it a moment to clean up
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    if (!pythonProcess.killed) {
      pythonProcess.kill('SIGKILL');
    }
  }
  
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.destroy();
  }
  
  app.quit();
}

// App lifecycle
app.whenReady().then(async () => {
  try {
    await createWindow();
  } catch (err) {
    console.error('Failed to start application:', err);
    dialog.showErrorBox(
      'Startup Error',
      `Failed to start the application:\n\n${err.message}\n\nPlease check the logs and try again.`
    );
    app.quit();
  }
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    shutdown();
  }
});

app.on('activate', () => {
  if (mainWindow === null) {
    createWindow();
  }
});

// IPC handlers for clean shutdown
ipcMain.on('quit-app', async () => {
  await shutdown();
});

// Handle crashes
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err);
  if (mainWindow && !mainWindow.isDestroyed()) {
    dialog.showErrorBox('Application Error', `An unexpected error occurred:\n\n${err.message}`);
  }
  shutdown();
});

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);