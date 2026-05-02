const nodemailer = require('nodemailer');

const transporter = nodemailer.createTransport({
  host:   process.env.SMTP_HOST || 'mailhog',
  port:   parseInt(process.env.SMTP_PORT) || 1025,
  secure: false,
  ...(process.env.SMTP_USER && {
    auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
  }),
});

async function sendEmail({ to, subject, html }) {
  await transporter.sendMail({
    from: `"Tienda Online" <${process.env.SMTP_FROM || 'noreply@tienda.com'}>`,
    to,
    subject,
    html,
  });
  console.log(`[Notification] Email sent → to: ${to} | subject: "${subject}"`);
}

module.exports = { sendEmail };
