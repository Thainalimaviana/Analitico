document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll("p.valor, td").forEach(el => {
    const textoOriginal = el.innerText.trim();

    if (!textoOriginal.includes("R$")) return;

    let texto = textoOriginal.replace(/[R$\s]/g, "");

    if (texto.includes(",") && texto.includes(".")) {
      if (texto.indexOf(",") > texto.indexOf(".")) {
        texto = texto.replace(/\./g, "").replace(",", ".");
      } else {
        texto = texto.replace(/,/g, "");
      }
    } else if (texto.includes(",")) {
      texto = texto.replace(",", ".");
    } else {
      texto = texto.replace(/,/g, "");
    }

    const num = parseFloat(texto);
    if (!isNaN(num)) {
      el.innerText = num.toLocaleString("pt-BR", {
        style: "currency",
        currency: "BRL"
      });
    }
  });
});
