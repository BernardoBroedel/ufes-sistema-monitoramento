package br.ufes.soe.ar.infra;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Guarda quais leituras ja foram publicadas, para que o polling de cinco em cinco
 * minutos sobre uma API que atualiza de hora em hora nao gere eventos repetidos.
 *
 * <p>O registro e persistido em disco porque um conjunto apenas em memoria perde
 * o historico a cada reinicio do produtor — e ao voltar, ele republica todas as
 * leituras da hora corrente. Isso foi observado na pratica: duas execucoes
 * seguidas do produtor deixaram as mesmas oito leituras duplicadas no topico.
 *
 * <p>O arquivo e podado para nao crescer sem limite; guardamos apenas as ultimas
 * {@value #MAXIMO} chaves, o que cobre varias horas das oito estacoes.
 */
public class RegistroDeduplicacao {

    private static final int MAXIMO = 500;

    private final Path arquivo;
    private final Set<String> chaves = new HashSet<>();

    public RegistroDeduplicacao(String caminho) {
        this.arquivo = Path.of(caminho);
        carregar();
    }

    private void carregar() {
        try {
            if (Files.exists(arquivo)) {
                chaves.addAll(Files.readAllLines(arquivo, StandardCharsets.UTF_8));
                System.out.println("dedup: " + chaves.size()
                        + " leituras ja publicadas, recuperadas de " + arquivo);
            }
        } catch (IOException e) {
            System.err.println("dedup: nao foi possivel ler " + arquivo
                    + " (" + e.getMessage() + "), comecando vazio");
        }
    }

    /** true se a chave e inedita — e, nesse caso, passa a ser considerada publicada. */
    public boolean registrarSeNova(String chave) {
        return chaves.add(chave);
    }

    /** Grava o estado atual. Chamado ao fim de cada ciclo de coleta. */
    public void salvar() {
        try {
            if (arquivo.getParent() != null) {
                Files.createDirectories(arquivo.getParent());
            }
            List<String> recentes = chaves.stream()
                    .sorted()
                    .skip(Math.max(0, chaves.size() - MAXIMO))
                    .toList();
            Files.write(arquivo, recentes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("dedup: falha ao salvar " + arquivo + ": " + e.getMessage());
        }
    }

    public int tamanho() {
        return chaves.size();
    }
}
