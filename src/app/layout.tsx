import "@/app/globals.css";

export const metadata = {
  title: "haste",
  description: "Share your goddamn code easily",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <meta name="robots" content="noindex,nofollow" />
      </head>
      <body className="w-full h-full p-5">
        <div className="h-screen flex flex-col">
          <main className="flex flex-auto w-full overflow-hidden pb-5">
            {children}
          </main>
        </div>
      </body>
    </html>
  );
}
