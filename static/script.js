/**
 * Handles front-end tab switching on the configuration dashboard
 * @param {string} tabId - The ID selector of the container element to display
 */
function showTab(tabId) {
    document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.nav-btn').forEach(el => el.classList.remove('active'));
    document.getElementById(tabId).classList.add('active');
    event.target.classList.add('active');
}