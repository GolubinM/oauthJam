function checkAccess() {
  const webAppUrl = 'https://script.google.com/macros/s/AKfycbzSiS3hK8cRcfKRiCQo2GyiuxbRm3xhbPrXuwQ7NSlujM5Udr-YiC-1bedXP7Ms0ASb/exec';
  const token = ScriptApp.getOAuthToken();
  const options = {
    method: 'post',
    contentType: 'application/json',
    headers: { Authorization: 'Bearer ' + token },
    muteHttpExceptions: true
  };

  // try {
  const response = UrlFetchApp.fetch(webAppUrl, options);
  const content = response.getContentText();  //console.log(content); // DEBUG console
  const code = response.getResponseCode(); // console.log("code:", code); // DEBUG console
  const needsAuth = [401, 403].includes(code) || isAuthPage_(content);
  if (needsAuth) {
    inform(SS, ['Необходимо авторизоваться: Меню->Авторизация'])
    throw new Error()
  }

  let { isAllowed, msg } = JSON.parse(content);
  if (!isAllowed) {
    inform(SS, [msg, "Внимание:"])
    throw new Error('Отсутствует доступ к библиотеке. Обратитесь в техподдержку.')
  }
  return isAllowed
  // } catch (e) {
  //   console.error('Ошибка вызова: ' + e.message);
  //   return null;
  // }
}

// ###  Проверка HTML страницы авторизации
function isAuthPage_(content) { return content.includes('<title>Sign in - Google Accounts</title>') || content.includes('<html>') }

// ## Проверка oauthScopes[]
function checkOAuthMenu_() {
  checkOAuth_((inf = true));
}
function checkOAuth_(inf = false) {
  const SS = SpreadsheetApp.getActive();
  const authInfo = ScriptApp.getAuthorizationInfo(ScriptApp.AuthMode.FULL);
  if (authInfo.getAuthorizationStatus() === ScriptApp.AuthorizationStatus.NOT_REQUIRED) {
    if (inf) {
      inform(SS, ["Авторизация не требуется.", "Проверка авторизации."]);
    }
    console.log(`Authorized all scopes successfully.`);
  } else {
    const scopesGranted = ScriptApp.getAuthorizationInfo(ScriptApp.AuthMode.FULL).getAuthorizedScopes();
    console.log(`Authorized scopes: ${scopesGranted} not enough.`);
    inform(SS, [`Требуется авторизация.`, "Проверка авторизации."]);
    ScriptApp.requireAllScopes(ScriptApp.AuthMode.FULL);
  }
}

function getAuthorizationHtml() {
  return HtmlService
    .createTemplateFromFile('authorization') // Файл ДОЛЖЕН быть в проекте библиотеки
    .evaluate()
    .getContent();
}

function auth() {
  SpreadsheetApp.getUi().showModalDialog(HtmlService.createHtmlOutput(getAuthorizationHtml()).setHeight(90), 'Авторизация');
}