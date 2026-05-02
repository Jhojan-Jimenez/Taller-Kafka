const kafka = require('../kafka');
const { sendEmail } = require('../mailer');

const consumer = kafka.consumer({ groupId: 'notification-group' });

const styles = `
  font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;
  padding: 20px; border: 1px solid #e0e0e0; border-radius: 8px;
`;
const headingStyle = 'color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;';
const tableStyle   = 'width:100%; border-collapse:collapse; margin:16px 0;';
const thStyle      = 'background:#3498db; color:#fff; padding:8px 12px; text-align:left;';
const tdStyle      = 'padding:8px 12px; border-bottom:1px solid #eee;';
const totalStyle   = 'background:#f8f9fa; font-weight:bold; padding:12px; border-radius:4px; margin:12px 0;';
const badgeStyle   = 'display:inline-block; background:#27ae60; color:#fff; padding:4px 10px; border-radius:12px; font-size:13px;';
const warnStyle    = 'display:inline-block; background:#e74c3c; color:#fff; padding:4px 10px; border-radius:12px; font-size:13px;';

function orderCreatedHtml({ customerName, orderId, items, totalAmount }) {
  const rows = items.map(i =>
    `<tr><td style="${tdStyle}">${i.productName}</td><td style="${tdStyle}">${i.quantity}</td><td style="${tdStyle}">$${i.unitPrice.toFixed(2)}</td><td style="${tdStyle}">$${(i.unitPrice * i.quantity).toFixed(2)}</td></tr>`
  ).join('');
  return `<div style="${styles}">
    <h2 style="${headingStyle}">Pedido recibido</h2>
    <p>Hola <strong>${customerName}</strong>, tu pedido ha sido registrado exitosamente.</p>
    <p><strong>ID de pedido:</strong> <code>${orderId}</code></p>
    <table style="${tableStyle}">
      <tr><th style="${thStyle}">Producto</th><th style="${thStyle}">Cant.</th><th style="${thStyle}">Precio unit.</th><th style="${thStyle}">Subtotal</th></tr>
      ${rows}
    </table>
    <div style="${totalStyle}">Total: $${totalAmount.toFixed(2)}</div>
    <p>Te notificaremos cuando tu pago sea procesado.</p>
  </div>`;
}

function paymentProcessedHtml({ customerName, orderId, amount, method }) {
  return `<div style="${styles}">
    <h2 style="${headingStyle}">Pago confirmado</h2>
    <p>Hola <strong>${customerName}</strong>,</p>
    <p>Tu pago fue procesado exitosamente. <span style="${badgeStyle}">APROBADO</span></p>
    <p><strong>Pedido:</strong> <code>${orderId}</code></p>
    <p><strong>Monto cobrado:</strong> $${parseFloat(amount).toFixed(2)}</p>
    <p><strong>Método:</strong> ${method}</p>
    <p>Estamos validando el inventario y preparando tu envío.</p>
  </div>`;
}

function shipmentCreatedHtml({ customerName, orderId, trackingNumber, estimatedDelivery, customerAddress }) {
  return `<div style="${styles}">
    <h2 style="${headingStyle}">Tu pedido está en camino</h2>
    <p>Hola <strong>${customerName}</strong>,</p>
    <p>Tu pedido ha sido enviado. <span style="${badgeStyle}">EN CAMINO</span></p>
    <p><strong>Pedido:</strong> <code>${orderId}</code></p>
    <p><strong>Número de rastreo:</strong> <strong style="font-size:18px;">${trackingNumber}</strong></p>
    <p><strong>Entrega estimada:</strong> ${estimatedDelivery}</p>
    <p><strong>Dirección de entrega:</strong> ${customerAddress}</p>
    <p>Gracias por tu compra.</p>
  </div>`;
}

function stockInsufficientHtml({ customerName, orderId }) {
  return `<div style="${styles}">
    <h2 style="${headingStyle}">Problema con tu pedido</h2>
    <p>Hola <strong>${customerName}</strong>,</p>
    <p>Lamentamos informarte que no hay stock suficiente para completar tu pedido. <span style="${warnStyle}">SIN STOCK</span></p>
    <p><strong>Pedido:</strong> <code>${orderId}</code></p>
    <p>Nuestro equipo se pondrá en contacto contigo para gestionar un reembolso completo.</p>
  </div>`;
}

async function startConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topics: ['orders', 'payments', 'shipments'], fromBeginning: false });

  console.log("[Notification] Consumer started, listening on topics: 'orders', 'payments', 'shipments'");

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      let event;
      try {
        event = JSON.parse(message.value.toString());
      } catch {
        console.error('[Notification] Invalid message format');
        return;
      }

      const { eventType, data } = event;
      console.log(`[Notification] Received from topic '${topic}': ${eventType} — orderId: ${data.orderId}`);

      try {
        if (eventType === 'OrderCreated') {
          await sendEmail({
            to:      data.customerEmail,
            subject: `Pedido recibido #${data.orderId.slice(0, 8).toUpperCase()}`,
            html:    orderCreatedHtml(data),
          });
        } else if (eventType === 'PaymentProcessed' && data.status === 'SUCCESS') {
          await sendEmail({
            to:      data.customerEmail,
            subject: `Pago confirmado — Pedido #${data.orderId.slice(0, 8).toUpperCase()}`,
            html:    paymentProcessedHtml(data),
          });
        } else if (eventType === 'ShipmentCreated') {
          await sendEmail({
            to:      data.customerEmail,
            subject: `Tu pedido está en camino — Tracking: ${data.trackingNumber}`,
            html:    shipmentCreatedHtml(data),
          });
        } else if (eventType === 'StockInsufficient') {
          await sendEmail({
            to:      data.customerEmail,
            subject: `Sin stock para tu pedido #${data.orderId.slice(0, 8).toUpperCase()}`,
            html:    stockInsufficientHtml(data),
          });
        }
      } catch (err) {
        console.error(`[Notification] Error sending email for ${eventType}:`, err.message);
      }
    },
  });
}

module.exports = { startConsumer };
