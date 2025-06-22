const fs = require("fs");
const path = require("path");
const { exec } = require("child_process");
const JavaScriptObfuscator = require("javascript-obfuscator");

const srcDir = path.join(__dirname, "srcBundle");
const distDir = path.join(__dirname, "destBundle");
const configPath = path.join(__dirname, "obfuscatorConfig.json");

// Читаем конфиг обфускатора
const obfuscatorConfig = JSON.parse(fs.readFileSync(configPath, "utf8"));

// Создаем папку dist, если нет
if (!fs.existsSync(distDir)) {
  fs.mkdirSync(distDir);
}

// Обфусцируем все .js файлы из srcBun
fs.readdirSync(srcDir).forEach((file) => {
  if (path.extname(file) === ".js" && path.basename(file) !== "recalcCost.js") {
    const filePath = path.join(srcDir, file);
    const sourceCode = fs.readFileSync(filePath, "utf8");

    // Обфусцируем
    const obfuscatedCode = JavaScriptObfuscator.obfuscate(sourceCode, obfuscatorConfig).getObfuscatedCode();

    // Записываем результат в dist
    fs.writeFileSync(path.join(distDir, file), obfuscatedCode, "utf8");
    console.log(`Обфусцирован: ${file}`);
  } else {
    const filePath = path.join(srcDir, file);
    const sourceCode = fs.readFileSync(filePath, "utf8");
    fs.writeFileSync(path.join(distDir, file), sourceCode, "utf8");
    console.log(`Перенесен файл: ${path.basename(file)}`);
  }
});

// Копируем остальные файлы из src, если нужны (например файлы JSON и т.п.)
// Опционально сюда можно добавить дополнительные копирования.

// Запускаем clasp push из папки dist
// exec('clasp push', { cwd: __dirname }, (error, stdout, stderr) => {
//   if (error) {
//     console.error(`Ошибка при поднятии проекта: ${error.message}`);
//     return;
//   }
//   if (stderr) {
//     console.error(`stderr: ${stderr}`);
//   }
//   console.log(`Результат push:\n${stdout}`);
// });
